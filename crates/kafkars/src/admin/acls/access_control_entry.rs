//! Owned principal, host, operation, and permission for one ACL entry.

use super::{AclOperation, AclPermissionType};

/// One concrete access-control decision.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AccessControlEntry {
    principal: String,
    host: String,
    operation: AclOperation,
    permission_type: AclPermissionType,
}

impl AccessControlEntry {
    /// Creates inert owned access-control intent.
    pub fn new(
        principal: impl Into<String>,
        host: impl Into<String>,
        operation: AclOperation,
        permission_type: AclPermissionType,
    ) -> Self {
        Self {
            principal: principal.into(),
            host: host.into(),
            operation,
            permission_type,
        }
    }

    /// Returns the principal spelling, such as `User:alice`.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Returns the host selector, including an explicit `*` wildcard.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the exact Kafka operation code.
    pub const fn operation(&self) -> AclOperation {
        self.operation
    }

    /// Returns the exact Kafka permission-type code.
    pub const fn permission_type(&self) -> AclPermissionType {
        self.permission_type
    }

    /// Reports whether this value can be used in a concrete ACL binding.
    pub fn is_valid_for_binding(&self) -> bool {
        !self.principal.is_empty()
            && !self.host.is_empty()
            && self.operation.is_valid_for_binding()
            && self.permission_type.is_valid_for_binding()
    }

    /// Consumes this entry into stable wire-free parts.
    pub fn into_parts(self) -> (String, String, AclOperation, AclPermissionType) {
        (
            self.principal,
            self.host,
            self.operation,
            self.permission_type,
        )
    }
}
