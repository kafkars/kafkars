//! Stable generated-free configuration-resource identities.

/// One canonical Kafka configuration-resource identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListConfigResource {
    pub(super) resource_type: i8,
    pub(super) name: String,
}

impl ListConfigResource {
    /// Returns Kafka's exact positive resource-type code.
    pub const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the exact resource name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Consumes the identity into stable generated-free parts.
    pub fn into_parts(self) -> (i8, String) {
        (self.resource_type, self.name)
    }
}
