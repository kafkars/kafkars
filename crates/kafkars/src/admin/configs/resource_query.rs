//! Stable configuration-resource query prepared before generic `DescribeConfigs` submission.

use crate::admin::{ConfigResource, ConfigResourceType};

/// One configuration resource and its optional ordered key selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigResourceQuery {
    resource_type: ConfigResourceType,
    resource_name: String,
    configuration_keys: Option<Vec<String>>,
}

impl ConfigResourceQuery {
    /// Requests every configuration for one exact resource identity.
    pub fn new(resource_type: ConfigResourceType, resource_name: impl Into<String>) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            configuration_keys: None,
        }
    }

    /// Restricts the response to the supplied configuration keys in this order.
    #[must_use]
    pub fn configuration_keys<I, T>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.configuration_keys = Some(keys.into_iter().map(Into::into).collect());
        self
    }

    /// Returns Kafka's exact requested resource type.
    pub const fn resource_type(&self) -> ConfigResourceType {
        self.resource_type
    }

    /// Returns the exact requested resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Returns `None` for all keys or the exact requested key order.
    pub fn selected_configuration_keys(&self) -> Option<&[String]> {
        self.configuration_keys.as_deref()
    }

    pub(crate) fn into_parts(self) -> (ConfigResourceType, String, Option<Vec<String>>) {
        (
            self.resource_type,
            self.resource_name,
            self.configuration_keys,
        )
    }
}

impl From<ConfigResource> for ConfigResourceQuery {
    fn from(resource: ConfigResource) -> Self {
        let (resource_type, resource_name) = resource.into_parts();
        Self::new(resource_type, resource_name)
    }
}
