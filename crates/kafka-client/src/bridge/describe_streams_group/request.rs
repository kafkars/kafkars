//! Linear public `StreamsGroup` description intent translated at submission.

use super::engine::Request as EngineRequest;

/// Request retained by the inert public builder before submission.
pub(crate) struct DescribeStreamsGroupAdminRequest {
    group_id: String,
    include_authorized_operations: bool,
    include_topology_description: bool,
}

impl DescribeStreamsGroupAdminRequest {
    pub(crate) const fn new(group_id: String) -> Self {
        Self {
            group_id,
            include_authorized_operations: false,
            include_topology_description: false,
        }
    }

    pub(crate) fn set_include_authorized_operations(&mut self, include: bool) {
        self.include_authorized_operations = include;
    }

    pub(crate) fn set_include_topology_description(&mut self, include: bool) {
        self.include_topology_description = include;
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(self.group_id)
            .with_authorized_operations(self.include_authorized_operations)
            .with_topology_description(self.include_topology_description)
    }
}

impl std::fmt::Debug for DescribeStreamsGroupAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeStreamsGroupAdminRequest")
            .field("group_id", &self.group_id)
            .field(
                "include_authorized_operations",
                &self.include_authorized_operations,
            )
            .field(
                "include_topology_description",
                &self.include_topology_description,
            )
            .finish()
    }
}
