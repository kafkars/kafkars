//! Inert public client-quota filter translated only at the engine boundary.

use crate::admin::{ClientQuotaFilterComponent, ClientQuotaMatch};

use super::engine::{
    FilterComponent as EngineFilterComponent, Match as EngineMatch, Request as EngineRequest,
};

/// Client-quota filter retained by the public builder.
pub(crate) struct DescribeClientQuotasAdminRequest {
    components: Vec<ClientQuotaFilterComponent>,
    strict: bool,
}

impl DescribeClientQuotasAdminRequest {
    pub(crate) const fn new(components: Vec<ClientQuotaFilterComponent>) -> Self {
        Self {
            components,
            strict: false,
        }
    }

    pub(crate) const fn with_strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(
            self.components
                .into_iter()
                .map(translate_component)
                .collect(),
            self.strict,
        )
    }
}

impl std::fmt::Debug for DescribeClientQuotasAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeClientQuotasAdminRequest")
            .field("components", &self.components)
            .field("strict", &self.strict)
            .finish()
    }
}

fn translate_component(component: ClientQuotaFilterComponent) -> EngineFilterComponent {
    let (entity_type, selection) = component.into_parts();
    EngineFilterComponent::new(entity_type, translate_match(selection))
}

fn translate_match(selection: ClientQuotaMatch) -> EngineMatch {
    match selection {
        ClientQuotaMatch::Exact(name) => EngineMatch::Exact(name),
        ClientQuotaMatch::Default => EngineMatch::Default,
        ClientQuotaMatch::AnySpecified => EngineMatch::AnySpecified,
    }
}
