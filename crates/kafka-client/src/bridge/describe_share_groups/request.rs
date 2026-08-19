//! Linear caller-ordered `ShareGroup` intent translated only at submission.

use super::engine::Request as EngineRequest;

/// Request retained by the inert public builder before submission.
pub(crate) struct DescribeShareGroupsAdminRequest {
    group_ids: Vec<String>,
    include_authorized_operations: bool,
}

impl DescribeShareGroupsAdminRequest {
    pub(crate) const fn new(group_ids: Vec<String>) -> Self {
        Self {
            group_ids,
            include_authorized_operations: false,
        }
    }

    pub(crate) fn set_include_authorized_operations(&mut self, include: bool) {
        self.include_authorized_operations = include;
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(self.group_ids)
            .with_authorized_operations(self.include_authorized_operations)
    }
}

impl std::fmt::Debug for DescribeShareGroupsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeShareGroupsAdminRequest")
            .field("group_ids", &self.group_ids)
            .field(
                "include_authorized_operations",
                &self.include_authorized_operations,
            )
            .finish()
    }
}
