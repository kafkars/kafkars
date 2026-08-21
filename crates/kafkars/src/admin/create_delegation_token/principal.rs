//! Stable inert Kafka principal vocabulary for delegation tokens.

/// One exact Kafka principal type and name.
///
/// Construction is inert. Empty, oversized, duplicate, and version-specific
/// request validation occurs only when the surrounding builder is submitted.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DelegationTokenPrincipal {
    principal_type: String,
    principal_name: String,
}

impl DelegationTokenPrincipal {
    /// Creates one inert principal without beginning an operation.
    pub fn new(principal_type: impl Into<String>, principal_name: impl Into<String>) -> Self {
        Self {
            principal_type: principal_type.into(),
            principal_name: principal_name.into(),
        }
    }

    /// Returns Kafka's exact principal type.
    pub fn principal_type(&self) -> &str {
        &self.principal_type
    }

    /// Returns Kafka's exact principal name.
    pub fn principal_name(&self) -> &str {
        &self.principal_name
    }

    pub(crate) fn into_parts(self) -> (String, String) {
        (self.principal_type, self.principal_name)
    }
}
