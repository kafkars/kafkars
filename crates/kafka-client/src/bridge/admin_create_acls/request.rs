//! Inert public ACL bindings translated only at the engine boundary.

use crate::admin::AclBinding;

use super::engine::{Binding as EngineBinding, Request as EngineRequest};

/// Caller-ordered concrete ACL bindings retained by the public builder.
pub(crate) struct CreateAclsAdminRequest {
    bindings: Vec<AclBinding>,
}

impl CreateAclsAdminRequest {
    pub(crate) const fn new(bindings: Vec<AclBinding>) -> Self {
        Self { bindings }
    }

    pub(in crate::bridge) const fn binding_count(&self) -> usize {
        self.bindings.len()
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(self.bindings.into_iter().map(translate_binding).collect())
    }
}

impl std::fmt::Debug for CreateAclsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CreateAclsAdminRequest")
            .field("bindings", &self.bindings)
            .finish()
    }
}

fn translate_binding(binding: AclBinding) -> EngineBinding {
    let (pattern, entry) = binding.into_parts();
    let (resource_type, resource_name, pattern_type) = pattern.into_parts();
    let (principal, host, operation, permission_type) = entry.into_parts();
    EngineBinding::new(
        resource_type.code(),
        resource_name,
        pattern_type.code(),
        principal,
        host,
        operation.code(),
        permission_type.code(),
    )
}
