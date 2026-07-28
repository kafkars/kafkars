//! Borrowed filter input and generated-free normalized ACL facts.

/// One borrowed API-key 29 filter without generated protocol ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DescribeAclsFilterRef<'a> {
    resource_type: i8,
    resource_name: Option<&'a str>,
    pattern_type: i8,
    principal: Option<&'a str>,
    host: Option<&'a str>,
    operation: i8,
    permission_type: i8,
}

impl<'a> DescribeAclsFilterRef<'a> {
    /// Retains exact signed Kafka enum codes and borrows every optional string.
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

/// One exact ACL binding detached from generated response grouping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedAclBinding {
    pub(super) resource_type: i8,
    pub(super) resource_name: String,
    pub(super) pattern_type: i8,
    pub(super) principal: String,
    pub(super) host: String,
    pub(super) operation: i8,
    pub(super) permission_type: i8,
}

impl NormalizedAclBinding {
    /// Consumes the binding into stable scalar facts.
    pub(crate) fn into_parts(self) -> (i8, String, i8, String, String, i8, i8) {
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

/// One validated API-key 29 response with exact broker facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeAclsResponse {
    pub(super) throttle_time_ms: u32,
    pub(super) error_code: i16,
    pub(super) error_message: Option<String>,
    pub(super) error_message_truncated: bool,
    pub(super) bindings: Vec<NormalizedAclBinding>,
    pub(super) retained_bytes: usize,
}

impl NormalizedDescribeAclsResponse {
    /// Consumes normalized ownership without exposing generated DTOs.
    pub(crate) fn into_parts(
        self,
    ) -> (
        u32,
        i16,
        Option<String>,
        bool,
        Vec<NormalizedAclBinding>,
        usize,
    ) {
        (
            self.throttle_time_ms,
            self.error_code,
            self.error_message,
            self.error_message_truncated,
            self.bindings,
            self.retained_bytes,
        )
    }
}
