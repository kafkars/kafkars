//! Engine-owned caller-positioned ACL deletion filters.

use kafka_client_core::{
    DeleteAclsFilter as CoreFilter, DeleteAclsPlan as CorePlan,
    DeleteAclsPlanError as CorePlanError,
};

/// One nullable ACL deletion filter using exact protocol-domain scalars.
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
    /// Creates inert owned filter intent for validation at admission.
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

    /// Consumes this filter into stable exact and nullable parts.
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

    fn canonicalize(&mut self) {
        canonicalize_optional(&mut self.resource_name);
        canonicalize_optional(&mut self.principal);
        canonicalize_optional(&mut self.host);
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

/// One nonempty caller-positioned ACL deletion batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteAclsRequest {
    filters: Vec<DeleteAclsFilter>,
}

impl DeleteAclsRequest {
    /// Creates inert request intent while preserving duplicate filter positions.
    pub const fn new(filters: Vec<DeleteAclsFilter>) -> Self {
        Self { filters }
    }

    /// Returns filters in exact caller order, including duplicates.
    pub fn filters(&self) -> &[DeleteAclsFilter] {
        &self.filters
    }

    /// Consumes this request into caller-positioned stable filters.
    pub fn into_filters(self) -> Vec<DeleteAclsFilter> {
        self.filters
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        for filter in &mut self.filters {
            filter.canonicalize();
        }
        self.filters.shrink_to_fit();
        self
    }

    pub(crate) fn into_plan(self) -> Result<CorePlan, CorePlanError> {
        CorePlan::new(
            self.filters
                .into_iter()
                .map(DeleteAclsFilter::into_core)
                .collect(),
        )
    }
}

fn canonicalize_optional(value: &mut Option<String>) {
    if let Some(owned) = value.take() {
        *value = Some(owned.into_boxed_str().into_string());
    }
}
