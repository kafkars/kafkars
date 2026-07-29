//! Linear caller-ordered StreamsGroup intent translated only at submission.

use super::engine::Request as EngineRequest;

/// Request retained by the inert public builder before submission.
pub(crate) struct DescribeStreamsGroupsAdminRequest {
    group_ids: Vec<String>,
    include_authorized_operations: bool,
    include_topology_description: bool,
}

impl DescribeStreamsGroupsAdminRequest {
    pub(crate) const fn new(group_ids: Vec<String>) -> Self {
        Self {
            group_ids,
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
        EngineRequest::new(self.group_ids)
            .with_authorized_operations(self.include_authorized_operations)
            .with_topology_description(self.include_topology_description)
    }
}

impl std::fmt::Debug for DescribeStreamsGroupsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeStreamsGroupsAdminRequest")
            .field("group_ids", &self.group_ids)
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
