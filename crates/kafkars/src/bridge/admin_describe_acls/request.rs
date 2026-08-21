//! Inert public ACL filter translated only at the engine boundary.

use crate::admin::AclBindingFilter;

use super::engine::{Filter as EngineFilter, Request as EngineRequest};

/// Exact ACL filter retained by the public builder.
pub(crate) struct DescribeAclsAdminRequest {
    filter: AclBindingFilter,
}

impl DescribeAclsAdminRequest {
    pub(crate) const fn new(filter: AclBindingFilter) -> Self {
        Self { filter }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        let (
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission_type,
        ) = self.filter.into_parts();
        EngineRequest::new(EngineFilter::new(
            resource_type.code(),
            resource_name,
            pattern_type.code(),
            principal,
            host,
            operation.code(),
            permission_type.code(),
        ))
    }
}

impl std::fmt::Debug for DescribeAclsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeAclsAdminRequest")
            .field("filter", &self.filter)
            .finish()
    }
}
