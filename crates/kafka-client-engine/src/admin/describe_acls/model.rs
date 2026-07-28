//! Engine-owned exact filter intent for one Admin `DescribeAcls` query.

use kafka_client_core::{
    DescribeAclsFilter as CoreFilter, DescribeAclsPlan as CorePlan,
    DescribeAclsPlanError as CorePlanError,
};

/// Exact protocol-domain ACL selectors with owned nullable strings.
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
    /// Creates inert wire-free filter intent for validation at admission.
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

    /// Consumes this filter into stable exact scalar parts.
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

    fn canonicalize(mut self) -> Self {
        self.resource_name = self.resource_name.map(canonical_string);
        self.principal = self.principal.map(canonical_string);
        self.host = self.host.map(canonical_string);
        self
    }

    fn into_core(self) -> CoreFilter {
        let (
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission_type,
        ) = self.into_parts();
        CoreFilter::new(
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission_type,
        )
    }
}

/// One bounded, wire-free ACL description request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeAclsRequest {
    filter: DescribeAclsFilter,
}

impl DescribeAclsRequest {
    /// Creates inert request intent for validation at the operation boundary.
    pub const fn new(filter: DescribeAclsFilter) -> Self {
        Self { filter }
    }

    /// Returns the exact ACL selector.
    pub const fn filter(&self) -> &DescribeAclsFilter {
        &self.filter
    }

    /// Consumes this request into its stable filter value.
    pub fn into_filter(self) -> DescribeAclsFilter {
        self.filter
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.filter = self.filter.canonicalize();
        self
    }

    pub(crate) fn into_plan(self) -> Result<CorePlan, CorePlanError> {
        CorePlan::new(self.filter.into_core())
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}
