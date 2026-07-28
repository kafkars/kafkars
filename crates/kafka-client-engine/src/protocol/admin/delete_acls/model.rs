//! Borrowed filter input and generated-free normalized response ownership.

use kafka_client_core::DeleteAclFilterResult;

/// One borrowed ACL deletion filter preserving nullable selectors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeleteAclsFilterRef<'a> {
    resource_type: i8,
    resource_name: Option<&'a str>,
    pattern_type: i8,
    principal: Option<&'a str>,
    host: Option<&'a str>,
    operation: i8,
    permission_type: i8,
}

impl<'a> DeleteAclsFilterRef<'a> {
    /// Borrows one already-validated caller filter position.
    pub(crate) const fn new(
        resource_type: i8,
        resource_name: Option<&'a str>,
        pattern_type: i8,
        principal: Option<&'a str>,
        host: Option<&'a str>,
        operation: i8,
        permission_type: i8,
    ) -> Self {
        Self {
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission_type,
        }
    }

    pub(crate) const fn resource_type(self) -> i8 {
        self.resource_type
    }

    pub(crate) const fn resource_name(self) -> Option<&'a str> {
        self.resource_name
    }

    pub(crate) const fn pattern_type(self) -> i8 {
        self.pattern_type
    }

    pub(crate) const fn principal(self) -> Option<&'a str> {
        self.principal
    }

    pub(crate) const fn host(self) -> Option<&'a str> {
        self.host
    }

    pub(crate) const fn operation(self) -> i8 {
        self.operation
    }

    pub(crate) const fn permission_type(self) -> i8 {
        self.permission_type
    }
}

/// Validated response whose positional vectors are caller-prepared terminal storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDeleteAclsResponse {
    pub(super) throttle_time_ms: u32,
    pub(super) results: Vec<DeleteAclFilterResult>,
    pub(super) retained_bytes: usize,
}

impl NormalizedDeleteAclsResponse {
    /// Consumes normalized ownership without exposing generated DTOs.
    pub(crate) fn into_parts(self) -> (u32, Vec<DeleteAclFilterResult>, usize) {
        (self.throttle_time_ms, self.results, self.retained_bytes)
    }
}
