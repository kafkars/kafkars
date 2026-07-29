//! Engine-owned scalar intent for caller-ordered consumer-group description.

use kafka_client_core::{
    AdminDescribeConsumerGroupsPlan, AdminDescribeConsumerGroupsPlanError,
    AdminDescribeConsumerGroupsScope,
};

/// One caller-ordered inert request validated at the public operation boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeConsumerGroupsRequest {
    groups: Vec<String>,
    include_authorized_operations: bool,
}

impl DescribeConsumerGroupsRequest {
    /// Creates request intent without starting time or work.
    pub const fn new(groups: Vec<String>) -> Self {
        Self {
            groups,
            include_authorized_operations: false,
        }
    }

    /// Replaces authorization-bit expansion intent.
    pub const fn with_authorized_operations(mut self, include: bool) -> Self {
        self.include_authorized_operations = include;
        self
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.groups = self
            .groups
            .into_iter()
            .map(|group| group.into_boxed_str().into_string())
            .collect();
        self.groups.shrink_to_fit();
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<AdminDescribeConsumerGroupsPlan, AdminDescribeConsumerGroupsPlanError> {
        self.into_plan_with_scope(AdminDescribeConsumerGroupsScope::ModernFirst)
    }

    pub(crate) fn into_plan_with_scope(
        self,
        scope: AdminDescribeConsumerGroupsScope,
    ) -> Result<AdminDescribeConsumerGroupsPlan, AdminDescribeConsumerGroupsPlanError> {
        AdminDescribeConsumerGroupsPlan::with_scope(
            self.groups,
            self.include_authorized_operations,
            scope,
        )
    }
}
