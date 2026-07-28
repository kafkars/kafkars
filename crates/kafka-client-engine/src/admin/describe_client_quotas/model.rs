//! Engine-owned, wire-free filter intent for one Admin `DescribeClientQuotas` query.

use kafka_client_core::{
    ClientQuotaMatch as CoreMatch, DescribeClientQuotaFilterComponent as CoreFilterComponent,
    DescribeClientQuotasPlan as CorePlan, DescribeClientQuotasPlanError as CorePlanError,
};

/// How one quota entity type is selected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeClientQuotaMatch {
    /// Selects one exact non-default entity name.
    Exact(String),
    /// Selects the default entity for this type.
    Default,
    /// Selects every explicitly named entity for this type.
    AnySpecified,
}

impl DescribeClientQuotaMatch {
    /// Consumes this selection into stable core policy.
    fn into_core(self) -> CoreMatch {
        match self {
            Self::Exact(name) => CoreMatch::Exact(canonical_string(name)),
            Self::Default => CoreMatch::Default,
            Self::AnySpecified => CoreMatch::AnySpecified,
        }
    }
}

/// One quota-entity filter component.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeClientQuotaFilterComponent {
    entity_type: String,
    selection: DescribeClientQuotaMatch,
}

impl DescribeClientQuotaFilterComponent {
    /// Creates inert filter intent for validation at the operation boundary.
    pub fn new(entity_type: String, selection: DescribeClientQuotaMatch) -> Self {
        Self {
            entity_type,
            selection,
        }
    }

    /// Returns the quota entity type.
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    /// Returns how the quota entity is selected.
    pub const fn selection(&self) -> &DescribeClientQuotaMatch {
        &self.selection
    }

    /// Consumes this component into stable scalar parts.
    pub fn into_parts(self) -> (String, DescribeClientQuotaMatch) {
        (self.entity_type, self.selection)
    }

    fn into_core(self) -> CoreFilterComponent {
        CoreFilterComponent::new(
            canonical_string(self.entity_type),
            self.selection.into_core(),
        )
    }
}

/// One bounded, wire-free quota-description request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeClientQuotasRequest {
    components: Vec<DescribeClientQuotaFilterComponent>,
    strict: bool,
}

impl DescribeClientQuotasRequest {
    /// Creates inert request intent. An empty component set describes all quotas.
    pub fn new(components: Vec<DescribeClientQuotaFilterComponent>, strict: bool) -> Self {
        Self { components, strict }
    }

    /// Returns caller-ordered filter components.
    pub fn components(&self) -> &[DescribeClientQuotaFilterComponent] {
        &self.components
    }

    /// Returns whether entities with unspecified types must be excluded.
    pub const fn strict(&self) -> bool {
        self.strict
    }

    /// Consumes this request into stable scalar parts.
    pub fn into_parts(self) -> (Vec<DescribeClientQuotaFilterComponent>, bool) {
        (self.components, self.strict)
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.components.shrink_to_fit();
        self
    }

    pub(crate) fn into_plan(self) -> Result<CorePlan, CorePlanError> {
        let (components, strict) = self.into_parts();
        CorePlan::new(
            components
                .into_iter()
                .map(DescribeClientQuotaFilterComponent::into_core)
                .collect(),
            strict,
        )
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}
