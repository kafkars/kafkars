//! Borrowed binding input and generated-free caller-ordered result facts.

/// One borrowed concrete ACL binding without generated protocol ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CreateAclBindingRef<'a> {
    resource_type: i8,
    resource_name: &'a str,
    pattern_type: i8,
    principal: &'a str,
    host: &'a str,
    operation: i8,
    permission_type: i8,
}

impl<'a> CreateAclBindingRef<'a> {
    /// Borrows one already-validated binding while preserving exact scalar codes.
    pub(crate) const fn new(
        resource_type: i8,
        resource_name: &'a str,
        pattern_type: i8,
        principal: &'a str,
        host: &'a str,
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

    pub(crate) const fn resource_name(self) -> &'a str {
        self.resource_name
    }

    pub(crate) const fn pattern_type(self) -> i8 {
        self.pattern_type
    }

    pub(crate) const fn principal(self) -> &'a str {
        self.principal
    }

    pub(crate) const fn host(self) -> &'a str {
        self.host
    }

    pub(crate) const fn operation(self) -> i8 {
        self.operation
    }

    pub(crate) const fn permission_type(self) -> i8 {
        self.permission_type
    }
}

/// Borrowed bounded Kafka result for one caller-ordered binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedCreateAclResultRef<'a> {
    error_code: i16,
    error_message: Option<&'a str>,
    error_message_truncated: bool,
}

impl<'a> NormalizedCreateAclResultRef<'a> {
    pub(super) const fn new(
        error_code: i16,
        error_message: Option<&'a str>,
        error_message_truncated: bool,
    ) -> Self {
        Self {
            error_code,
            error_message,
            error_message_truncated,
        }
    }

    /// Consumes one view into exact signed and bounded diagnostic facts.
    pub(crate) const fn into_parts(self) -> (i16, Option<&'a str>, bool) {
        (
            self.error_code,
            self.error_message,
            self.error_message_truncated,
        )
    }
}
