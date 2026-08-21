//! Owned resource identity and matching form for one concrete ACL binding.

use super::{AclPatternType, AclResourceType};

/// One resource name interpreted using an exact Kafka pattern type.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ResourcePattern {
    resource_type: AclResourceType,
    name: String,
    pattern_type: AclPatternType,
}

impl ResourcePattern {
    /// Creates inert owned resource-pattern intent.
    pub fn new(
        resource_type: AclResourceType,
        name: impl Into<String>,
        pattern_type: AclPatternType,
    ) -> Self {
        Self {
            resource_type,
            name: name.into(),
            pattern_type,
        }
    }

    /// Returns the exact Kafka resource-type code.
    pub const fn resource_type(&self) -> AclResourceType {
        self.resource_type
    }

    /// Returns the owned resource name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the exact Kafka resource-pattern code.
    pub const fn pattern_type(&self) -> AclPatternType {
        self.pattern_type
    }

    /// Reports whether this value can be used in a concrete ACL binding.
    pub fn is_valid_for_binding(&self) -> bool {
        self.resource_type.is_valid_for_binding()
            && !self.name.is_empty()
            && self.pattern_type.is_valid_for_binding()
    }

    /// Consumes this pattern into stable wire-free parts.
    pub fn into_parts(self) -> (AclResourceType, String, AclPatternType) {
        (self.resource_type, self.name, self.pattern_type)
    }
}
