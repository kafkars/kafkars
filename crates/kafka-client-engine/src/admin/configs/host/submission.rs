//! Linear ownership of one route-local `DescribeConfigs` driver submission.

use kafka_client_core::{DescribeConfigsPlan, DescribeConfigsRoute, OperationId};

use crate::clock::OperationDeadline;

pub(crate) struct DescribeConfigsSubmission {
    pub(crate) operation_id: OperationId,
    pub(crate) deadline: OperationDeadline,
    pub(crate) route: DescribeConfigsRoute,
    pub(crate) plan: DescribeConfigsPlan,
    pub(crate) result_limit: usize,
}

impl DescribeConfigsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        DescribeConfigsRoute,
        DescribeConfigsPlan,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.route,
            self.plan,
            self.result_limit,
        )
    }
}
