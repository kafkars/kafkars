//! Validated caller-ordered ACL filters for one bounded deletion batch.

use core::fmt;

const MAX_ACL_FILTER_STRING_BYTES: usize = i16::MAX as usize;

/// Maximum filter positions retained by one deterministic deletion plan.
pub const MAX_DELETE_ACLS_FILTERS: usize = 16 * 1024;

/// One wire-free ACL deletion filter with exact protocol-domain scalars.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteAclsFilter {
    resource_type: i8,
    resource_name: Option<String>,
    pattern_type: i8,
    principal: Option<String>,
    host: Option<String>,
    operation: i8,
    permission_type: i8,
}

impl DeleteAclsFilter {
    /// Creates inert filter intent for validation by the enclosing plan.
    pub const fn new(
        resource_type: i8,
        resource_name: Option<String>,
        pattern_type: i8,
        principal: Option<String>,
        host: Option<String>,
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

    /// Returns Kafka's exact resource-type selector.
    pub const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the nullable resource-name selector.
    pub fn resource_name(&self) -> Option<&str> {
        self.resource_name.as_deref()
    }

    /// Returns Kafka's exact resource-pattern selector.
    pub const fn pattern_type(&self) -> i8 {
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

    /// Returns Kafka's exact ACL-operation selector.
    pub const fn operation(&self) -> i8 {
        self.operation
    }

    /// Returns Kafka's exact permission-type selector.
    pub const fn permission_type(&self) -> i8 {
        self.permission_type
    }

    /// Consumes this filter into adapter-owned exact parts.
    pub fn into_parts(
        self,
    ) -> (
        i8,
        Option<String>,
        i8,
        Option<String>,
        Option<String>,
        i8,
        i8,
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

/// Validated caller-ordered intent for one bounded `DeleteAcls` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteAclsPlan {
    filters: Vec<DeleteAclsFilter>,
}

impl DeleteAclsPlan {
    /// Validates every filter while deliberately preserving duplicates.
    pub fn new(filters: Vec<DeleteAclsFilter>) -> Result<Self, DeleteAclsPlanError> {
        if filters.is_empty() {
            return Err(DeleteAclsPlanError::EmptyBatch);
        }
        if filters.len() > MAX_DELETE_ACLS_FILTERS {
            return Err(DeleteAclsPlanError::BatchTooLarge);
        }
        for filter in &filters {
            validate_filter(filter)?;
        }
        Ok(Self { filters })
    }

    /// Returns filters in exact caller order, including duplicate positions.
    pub fn filters(&self) -> &[DeleteAclsFilter] {
        &self.filters
    }

    /// Returns exact outer result slots required before admission.
    pub const fn required_filter_result_capacity(&self) -> usize {
        self.filters.len()
    }

    pub(crate) fn into_filters(self) -> Vec<DeleteAclsFilter> {
        self.filters
    }
}

fn validate_filter(filter: &DeleteAclsFilter) -> Result<(), DeleteAclsPlanError> {
    validate_filter_scalar(
        filter.resource_type,
        DeleteAclsPlanError::InvalidResourceType,
    )?;
    validate_optional_string(
        filter.resource_name.as_deref(),
        DeleteAclsPlanError::EmptyResourceName,
        DeleteAclsPlanError::ResourceNameTooLong,
    )?;
    validate_filter_scalar(filter.pattern_type, DeleteAclsPlanError::InvalidPatternType)?;
    validate_optional_string(
        filter.principal.as_deref(),
        DeleteAclsPlanError::EmptyPrincipal,
        DeleteAclsPlanError::PrincipalTooLong,
    )?;
    validate_optional_string(
        filter.host.as_deref(),
        DeleteAclsPlanError::EmptyHost,
        DeleteAclsPlanError::HostTooLong,
    )?;
    validate_filter_scalar(filter.operation, DeleteAclsPlanError::InvalidOperation)?;
    validate_filter_scalar(
        filter.permission_type,
        DeleteAclsPlanError::InvalidPermissionType,
    )
}

fn validate_filter_scalar(
    value: i8,
    error: DeleteAclsPlanError,
) -> Result<(), DeleteAclsPlanError> {
    if value <= 0 {
        return Err(error);
    }
    Ok(())
}

fn validate_optional_string(
    value: Option<&str>,
    empty: DeleteAclsPlanError,
    too_long: DeleteAclsPlanError,
) -> Result<(), DeleteAclsPlanError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_empty() {
        return Err(empty);
    }
    if value.len() > MAX_ACL_FILTER_STRING_BYTES {
        return Err(too_long);
    }
    Ok(())
}

/// Invalid deterministic ACL-deletion filter intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteAclsPlanError {
    /// At least one filter position is required.
    EmptyBatch,
    /// The filter batch exceeds the fixed deterministic count limit.
    BatchTooLarge,
    /// Resource-type filters must be positive protocol-domain values.
    InvalidResourceType,
    /// A present resource name must not be empty.
    EmptyResourceName,
    /// A present resource name must fit Kafka's string domain.
    ResourceNameTooLong,
    /// Pattern-type filters must be positive protocol-domain values.
    InvalidPatternType,
    /// A present principal must not be empty.
    EmptyPrincipal,
    /// A present principal must fit Kafka's string domain.
    PrincipalTooLong,
    /// A present host must not be empty.
    EmptyHost,
    /// A present host must fit Kafka's string domain.
    HostTooLong,
    /// Operation filters must be positive protocol-domain values.
    InvalidOperation,
    /// Permission filters must be positive protocol-domain values.
    InvalidPermissionType,
}

impl fmt::Display for DeleteAclsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid DeleteAcls plan: {self:?}")
    }
}

impl std::error::Error for DeleteAclsPlanError {}
