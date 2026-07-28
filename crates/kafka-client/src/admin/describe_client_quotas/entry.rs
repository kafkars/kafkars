//! Stable canonical client-quota entity descriptions.

use super::ClientQuotaValue;

/// One entity-type component in a returned client-quota entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientQuotaEntityComponent {
    entity_type: String,
    entity_name: Option<String>,
}

impl ClientQuotaEntityComponent {
    pub(crate) const fn new(entity_type: String, entity_name: Option<String>) -> Self {
        Self {
            entity_type,
            entity_name,
        }
    }

    /// Identifies one explicitly named entity component.
    pub fn named(entity_type: impl Into<String>, entity_name: impl Into<String>) -> Self {
        Self::new(entity_type.into(), Some(entity_name.into()))
    }

    /// Identifies Kafka's unnamed default entity for one component type.
    pub fn default_entity(entity_type: impl Into<String>) -> Self {
        Self::new(entity_type.into(), None)
    }

    /// Returns Kafka's entity-type name.
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    /// Returns the explicit entity name, or `None` for the default entity.
    pub fn entity_name(&self) -> Option<&str> {
        self.entity_name.as_deref()
    }

    /// Consumes this component into its type and optional name.
    pub fn into_parts(self) -> (String, Option<String>) {
        (self.entity_type, self.entity_name)
    }
}

/// One canonical client-quota entity and all its quota values.
#[derive(Clone, Debug, PartialEq)]
pub struct ClientQuotaEntry {
    components: Vec<ClientQuotaEntityComponent>,
    values: Vec<ClientQuotaValue>,
}

impl ClientQuotaEntry {
    pub(crate) const fn new(
        components: Vec<ClientQuotaEntityComponent>,
        values: Vec<ClientQuotaValue>,
    ) -> Self {
        Self { components, values }
    }

    /// Returns entity components in entity-type byte order.
    pub fn components(&self) -> &[ClientQuotaEntityComponent] {
        &self.components
    }

    /// Returns quota key/value pairs in key byte order.
    pub fn values(&self) -> &[ClientQuotaValue] {
        &self.values
    }

    /// Consumes this entry into its canonical components and quota values.
    pub fn into_parts(self) -> (Vec<ClientQuotaEntityComponent>, Vec<ClientQuotaValue>) {
        (self.components, self.values)
    }
}
