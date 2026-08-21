//! Nullable ACL selection values for describe and delete operations.

use super::{AclBinding, AclOperation, AclPatternType, AclPermissionType, AclResourceType};

/// One wire-free ACL filter preserving Kafka's nullable string selectors.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AclBindingFilter {
    resource_type: AclResourceType,
    resource_name: Option<String>,
    pattern_type: AclPatternType,
    principal: Option<String>,
    host: Option<String>,
    operation: AclOperation,
    permission_type: AclPermissionType,
}

impl AclBindingFilter {
    /// Creates an ACL filter with nullable string selectors left unrestricted.
    pub const fn new(
        resource_type: AclResourceType,
        pattern_type: AclPatternType,
        operation: AclOperation,
        permission_type: AclPermissionType,
    ) -> Self {
        Self {
            resource_type,
            resource_name: None,
            pattern_type,
            principal: None,
            host: None,
            operation,
            permission_type,
        }
    }

    /// Creates a filter matching every valid ACL binding.
    pub const fn any() -> Self {
        Self::new(
            AclResourceType::ANY,
            AclPatternType::ANY,
            AclOperation::ANY,
            AclPermissionType::ANY,
        )
    }

    /// Creates an exact filter from one concrete binding.
    pub fn from_binding(binding: &AclBinding) -> Self {
        Self::new(
            binding.pattern().resource_type(),
            binding.pattern().pattern_type(),
            binding.entry().operation(),
            binding.entry().permission_type(),
        )
        .with_resource_name(binding.pattern().name())
        .with_principal(binding.entry().principal())
        .with_host(binding.entry().host())
    }

    /// Selects one exact resource name rather than every resource name.
    pub fn with_resource_name(mut self, resource_name: impl Into<String>) -> Self {
        self.resource_name = Some(resource_name.into());
        self
    }

    /// Selects one exact principal rather than every principal.
    pub fn with_principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    /// Selects one exact host rather than every host.
    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Returns the resource-type selector.
    pub const fn resource_type(&self) -> AclResourceType {
        self.resource_type
    }

    /// Returns the nullable resource-name selector.
    pub fn resource_name(&self) -> Option<&str> {
        self.resource_name.as_deref()
    }

    /// Returns the resource-pattern selector.
    pub const fn pattern_type(&self) -> AclPatternType {
        self.pattern_type
    }

    /// Returns the nullable principal selector.
    pub fn principal(&self) -> Option<&str> {
        self.principal.as_deref()
    }

    /// Returns the nullable host selector.
    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    /// Returns the operation selector.
    pub const fn operation(&self) -> AclOperation {
        self.operation
    }

    /// Returns the permission-type selector.
    pub const fn permission_type(&self) -> AclPermissionType {
        self.permission_type
    }

    /// Reports whether this value can be sent as an ACL filter.
    pub fn is_valid_for_filter(&self) -> bool {
        self.resource_type.is_valid_for_filter()
            && self.pattern_type.is_valid_for_filter()
            && self.operation.is_valid_for_filter()
            && self.permission_type.is_valid_for_filter()
            && optional_string_is_valid(self.resource_name.as_deref())
            && optional_string_is_valid(self.principal.as_deref())
            && optional_string_is_valid(self.host.as_deref())
    }

    /// Consumes this filter into stable wire-free parts.
    pub fn into_parts(
        self,
    ) -> (
        AclResourceType,
        Option<String>,
        AclPatternType,
        Option<String>,
        Option<String>,
        AclOperation,
        AclPermissionType,
    ) {
        (
            self.resource_type,
            self.resource_name,
            self.pattern_type,
            self.principal,
            self.host,
            self.operation,
            self.permission_type,
        )
    }
}

impl From<&AclBinding> for AclBindingFilter {
    fn from(binding: &AclBinding) -> Self {
        Self::from_binding(binding)
    }
}

fn optional_string_is_valid(value: Option<&str>) -> bool {
    match value {
        Some(value) => !value.is_empty(),
        None => true,
    }
}
