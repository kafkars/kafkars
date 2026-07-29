//! Linear public ShareGroup description intent translated at submission.

use super::engine::Request as EngineRequest;

/// Request retained by the inert public builder before submission.
pub(crate) struct DescribeShareGroupAdminRequest {
    group_id: String,
    include_authorized_operations: bool,
}

impl DescribeShareGroupAdminRequest {
    pub(crate) const fn new(group_id: String) -> Self {
        Self {
            group_id,
            include_authorized_operations: false,
        }
    }

    pub(crate) fn set_include_authorized_operations(&mut self, include: bool) {
        self.include_authorized_operations = include;
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(self.group_id)
            .with_authorized_operations(self.include_authorized_operations)
    }
}

impl std::fmt::Debug for DescribeShareGroupAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeShareGroupAdminRequest")
            .field("group_id", &self.group_id)
            .field(
                "include_authorized_operations",
                &self.include_authorized_operations,
            )
            .finish()
    }
}
