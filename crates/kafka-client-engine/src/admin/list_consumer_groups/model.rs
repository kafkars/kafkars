//! Engine-owned inert filters for one general `ListGroups` query.

use kafka_client_core::{AdminGroupListingFilters, AdminGroupListingFiltersError};

/// Stable generated-free filters retained until the public operation boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListGroupsRequest {
    states: Vec<String>,
    group_types: Vec<String>,
    protocol_types: Vec<String>,
}

impl AdminListGroupsRequest {
    /// Creates inert future-compatible string filters.
    pub const fn new(
        state_filters: Vec<String>,
        group_type_filters: Vec<String>,
        protocol_type_filters: Vec<String>,
    ) -> Self {
        Self {
            states: state_filters,
            group_types: group_type_filters,
            protocol_types: protocol_type_filters,
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        canonicalize(&mut self.states);
        canonicalize(&mut self.group_types);
        canonicalize(&mut self.protocol_types);
        self
    }

    pub(crate) fn into_filters(
        self,
    ) -> Result<AdminGroupListingFilters, AdminGroupListingFiltersError> {
        AdminGroupListingFilters::new(self.states, self.group_types, self.protocol_types)
    }
}

fn canonicalize(filters: &mut Vec<String>) {
    for filter in filters.iter_mut() {
        *filter = core::mem::take(filter).into_boxed_str().into_string();
    }
    filters.shrink_to_fit();
}
