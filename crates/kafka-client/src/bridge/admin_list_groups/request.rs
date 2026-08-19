//! Inert public `ListGroups` filters translated only at the engine boundary.

use kafka_client_engine::AdminListGroupsRequest as EngineRequest;

/// Linear request retained by the public builder before submission.
#[expect(
    clippy::struct_field_names,
    reason = "the fields preserve the three distinct Kafka ListGroups filter categories"
)]
pub(crate) struct ListGroupsAdminRequest {
    state_filters: Vec<String>,
    group_type_filters: Vec<String>,
    protocol_type_filters: Vec<String>,
}

impl ListGroupsAdminRequest {
    pub(crate) const fn new(
        state_filters: Vec<String>,
        group_type_filters: Vec<String>,
        protocol_type_filters: Vec<String>,
    ) -> Self {
        Self {
            state_filters,
            group_type_filters,
            protocol_type_filters,
        }
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(
            self.state_filters,
            self.group_type_filters,
            self.protocol_type_filters,
        )
    }
}

impl std::fmt::Debug for ListGroupsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListGroupsAdminRequest")
            .field("state_filters", &self.state_filters)
            .field("group_type_filters", &self.group_type_filters)
            .field("protocol_type_filters", &self.protocol_type_filters)
            .finish()
    }
}
