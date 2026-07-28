//! Inert public ACL filters translated only at the engine boundary.

use crate::admin::AclBindingFilter;

use super::engine::{Filter as EngineFilter, Request as EngineRequest};

/// Caller-positioned ACL filters retained by the public builder.
pub(crate) struct DeleteAclsAdminRequest {
    filters: Vec<AclBindingFilter>,
}

impl DeleteAclsAdminRequest {
    pub(crate) const fn new(filters: Vec<AclBindingFilter>) -> Self {
        Self { filters }
    }

    pub(in crate::bridge) const fn filter_count(&self) -> usize {
        self.filters.len()
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(self.filters.into_iter().map(translate_filter).collect())
    }
}

impl std::fmt::Debug for DeleteAclsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteAclsAdminRequest")
            .field("filters", &self.filters)
            .finish()
    }
}

fn translate_filter(filter: AclBindingFilter) -> EngineFilter {
    let (resource_type, resource_name, pattern_type, principal, host, operation, permission_type) =
        filter.into_parts();
    EngineFilter::new(
        resource_type.code(),
        resource_name,
        pattern_type.code(),
        principal,
        host,
        operation.code(),
        permission_type.code(),
    )
}
