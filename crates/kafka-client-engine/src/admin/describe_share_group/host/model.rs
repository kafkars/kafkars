//! Prepared API-77 request and exact driver-handoff ownership.

use kafka_client_core::{DescribeShareGroupPlan, OperationId};

use crate::clock::OperationDeadline;

/// Validated API-77 v0 request ready for the engine-host adapter.
pub(crate) struct DescribeShareGroupSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: DescribeShareGroupPlan,
    pub(super) result_limit: usize,
}

impl DescribeShareGroupSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        DescribeShareGroupPlan,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.result_limit,
        )
    }
}

pub(crate) enum DescribeShareGroupTurn {
    Idle,
    Progress,
    Submit(DescribeShareGroupSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DescribeShareGroupHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
