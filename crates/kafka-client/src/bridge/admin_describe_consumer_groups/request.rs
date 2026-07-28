//! Inert public request translated only at the engine boundary.

use kafka_client_engine::DescribeConsumerGroupsRequest as EngineRequest;

/// Linear caller-ordered request retained by the public builder.
pub(crate) struct DescribeConsumerGroupsAdminRequest {
    groups: Vec<String>,
    include_authorized_operations: bool,
}

impl DescribeConsumerGroupsAdminRequest {
    pub(crate) const fn new(groups: Vec<String>) -> Self {
        Self {
            groups,
            include_authorized_operations: false,
        }
    }

    pub(crate) fn set_include_authorized_operations(&mut self, include: bool) {
        self.include_authorized_operations = include;
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(self.groups)
            .with_authorized_operations(self.include_authorized_operations)
    }
}

impl std::fmt::Debug for DescribeConsumerGroupsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeConsumerGroupsAdminRequest")
            .field("groups", &self.groups)
            .field(
                "include_authorized_operations",
                &self.include_authorized_operations,
            )
            .finish()
    }
}
