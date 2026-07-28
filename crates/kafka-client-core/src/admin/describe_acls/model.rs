//! Validated wire-free filter intent for one ACL description query.

use core::fmt;

const MAX_ACL_FILTER_STRING_BYTES: usize = i16::MAX as usize;

/// Exact protocol-domain ACL filter values and nullable owned strings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeAclsFilter {
    resource_type: i8,
    resource_name: Option<String>,
    pattern_type: i8,
    principal: Option<String>,
    host: Option<String>,
    operation: i8,
    permission_type: i8,
}

impl DescribeAclsFilter {
    /// Creates inert exact filter intent for validation by the enclosing plan.
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

    /// Returns Kafka's exact resource-type filter value.
    pub const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the nullable resource-name filter.
    pub fn resource_name(&self) -> Option<&str> {
        self.resource_name.as_deref()
    }

    /// Returns Kafka's exact resource-pattern filter value.
    pub const fn pattern_type(&self) -> i8 {
        self.pattern_type
    }

    /// Returns the nullable principal filter.
    pub fn principal(&self) -> Option<&str> {
        self.principal.as_deref()
    }

    /// Returns the nullable host filter.
    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    /// Returns Kafka's exact ACL-operation filter value.
    pub const fn operation(&self) -> i8 {
        self.operation
    }

    /// Returns Kafka's exact permission-type filter value.
    pub const fn permission_type(&self) -> i8 {
        self.permission_type
    }

    /// Consumes this filter into adapter-owned protocol-domain parts.
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

/// Validated intent for one bounded read-only ACL query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeAclsPlan {
    filter: DescribeAclsFilter,
}

impl DescribeAclsPlan {
    /// Validates exact filter values without inventing wire or routing policy.
    pub fn new(filter: DescribeAclsFilter) -> Result<Self, DescribeAclsPlanError> {
        validate_filter_value(
            filter.resource_type,
            DescribeAclsPlanError::InvalidResourceTypeFilter,
        )?;
        validate_optional_string(
            filter.resource_name.as_deref(),
            DescribeAclsPlanError::EmptyResourceNameFilter,
            DescribeAclsPlanError::ResourceNameFilterTooLong,
        )?;
        validate_filter_value(
            filter.pattern_type,
            DescribeAclsPlanError::InvalidPatternTypeFilter,
        )?;
        validate_optional_string(
            filter.principal.as_deref(),
            DescribeAclsPlanError::EmptyPrincipalFilter,
            DescribeAclsPlanError::PrincipalFilterTooLong,
        )?;
        validate_optional_string(
            filter.host.as_deref(),
            DescribeAclsPlanError::EmptyHostFilter,
            DescribeAclsPlanError::HostFilterTooLong,
        )?;
        validate_filter_value(
            filter.operation,
            DescribeAclsPlanError::InvalidOperationFilter,
        )?;
        validate_filter_value(
            filter.permission_type,
            DescribeAclsPlanError::InvalidPermissionTypeFilter,
        )?;
        Ok(Self { filter })
    }

    /// Returns the exact validated ACL filter.
    pub const fn filter(&self) -> &DescribeAclsFilter {
        &self.filter
    }
}

fn validate_filter_value(
    value: i8,
    error: DescribeAclsPlanError,
) -> Result<(), DescribeAclsPlanError> {
    if value <= 0 {
        return Err(error);
    }
    Ok(())
}

fn validate_optional_string(
    value: Option<&str>,
    empty: DescribeAclsPlanError,
    too_long: DescribeAclsPlanError,
) -> Result<(), DescribeAclsPlanError> {
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

/// Invalid deterministic ACL filter intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeAclsPlanError {
    /// Resource-type filters must be positive protocol-domain values.
    InvalidResourceTypeFilter,
    /// A present resource-name filter must not be empty.
    EmptyResourceNameFilter,
    /// A present resource-name filter must fit the bounded string domain.
    ResourceNameFilterTooLong,
    /// Pattern-type filters must be positive protocol-domain values.
    InvalidPatternTypeFilter,
    /// A present principal filter must not be empty.
    EmptyPrincipalFilter,
    /// A present principal filter must fit the bounded string domain.
    PrincipalFilterTooLong,
    /// A present host filter must not be empty.
    EmptyHostFilter,
    /// A present host filter must fit the bounded string domain.
    HostFilterTooLong,
    /// Operation filters must be positive protocol-domain values.
    InvalidOperationFilter,
    /// Permission-type filters must be positive protocol-domain values.
    InvalidPermissionTypeFilter,
}

impl fmt::Display for DescribeAclsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid DescribeAcls filter: {self:?}")
    }
}

impl std::error::Error for DescribeAclsPlanError {}
