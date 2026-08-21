//! Concrete association between one resource pattern and ACL entry.

use super::{AccessControlEntry, ResourcePattern};

/// One complete ACL binding suitable for creation or exact result values.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AclBinding {
    pattern: ResourcePattern,
    entry: AccessControlEntry,
}

impl AclBinding {
    /// Creates one inert binding from owned stable values.
    pub const fn new(pattern: ResourcePattern, entry: AccessControlEntry) -> Self {
        Self { pattern, entry }
    }

    /// Returns the bound resource pattern.
    pub const fn pattern(&self) -> &ResourcePattern {
        &self.pattern
    }

    /// Returns the bound access-control entry.
    pub const fn entry(&self) -> &AccessControlEntry {
        &self.entry
    }

    /// Reports whether every field is valid for a concrete binding.
    pub fn is_valid_for_binding(&self) -> bool {
        self.pattern.is_valid_for_binding() && self.entry.is_valid_for_binding()
    }

    /// Consumes this binding into its stable owned values.
    pub fn into_parts(self) -> (ResourcePattern, AccessControlEntry) {
        (self.pattern, self.entry)
    }
}
