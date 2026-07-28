//! Engine-owned caller-ordered ACL creation intent.

use kafka_client_core::{
    CreateAclBinding as CoreBinding, CreateAclsPlan as CorePlan,
    CreateAclsPlanError as CorePlanError,
};

/// One concrete ACL binding using exact protocol-domain scalar values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAclBinding {
    resource_type: i8,
    resource_name: String,
    pattern_type: i8,
    principal: String,
    host: String,
    operation: i8,
    permission_type: i8,
}

impl CreateAclBinding {
    /// Creates inert owned binding intent for validation at admission.
    pub fn new(
        resource_type: i8,
        resource_name: impl Into<String>,
        pattern_type: i8,
        principal: impl Into<String>,
        host: impl Into<String>,
        operation: i8,
        permission_type: i8,
    ) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            pattern_type,
            principal: principal.into(),
            host: host.into(),
            operation,
            permission_type,
        }
    }

    /// Returns Kafka's exact concrete resource type.
    pub const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the concrete resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Returns Kafka's exact concrete resource-pattern type.
    pub const fn pattern_type(&self) -> i8 {
        self.pattern_type
    }

    /// Returns the principal identity.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Returns the host identity or explicit wildcard.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns Kafka's exact concrete operation.
    pub const fn operation(&self) -> i8 {
        self.operation
    }

    /// Returns Kafka's exact concrete permission type.
    pub const fn permission_type(&self) -> i8 {
        self.permission_type
    }

    /// Consumes this binding into stable exact scalar parts.
    pub fn into_parts(self) -> (i8, String, i8, String, String, i8, i8) {
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
        self.resource_name = canonical_string(self.resource_name);
        self.principal = canonical_string(self.principal);
        self.host = canonical_string(self.host);
        self
    }

    fn into_core(self) -> CoreBinding {
        let (
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission_type,
        ) = self.into_parts();
        CoreBinding::new(
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

/// One nonempty caller-ordered ACL creation batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAclsRequest {
    bindings: Vec<CreateAclBinding>,
}

impl CreateAclsRequest {
    /// Creates inert request intent for validation at the operation boundary.
    pub const fn new(bindings: Vec<CreateAclBinding>) -> Self {
        Self { bindings }
    }

    /// Returns requested bindings in exact caller order.
    pub fn bindings(&self) -> &[CreateAclBinding] {
        &self.bindings
    }

    /// Consumes this request into caller-ordered stable bindings.
    pub fn into_bindings(self) -> Vec<CreateAclBinding> {
        self.bindings
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.bindings = self
            .bindings
            .into_iter()
            .map(CreateAclBinding::canonicalize)
            .collect::<Vec<_>>()
            .into_boxed_slice()
            .into_vec();
        self
    }

    pub(crate) fn into_plan(self) -> Result<CorePlan, CorePlanError> {
        CorePlan::new(
            self.bindings
                .into_iter()
                .map(CreateAclBinding::into_core)
                .collect(),
        )
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}
