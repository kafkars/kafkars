//! Prepared API-89 request and exact driver-handoff ownership.

use kafka_client_core::{DescribeStreamsGroupPlan, OperationId};

use crate::clock::OperationDeadline;

/// Validated API-89 v0-v1 request ready for the engine-host adapter.
pub(crate) struct DescribeStreamsGroupSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: DescribeStreamsGroupPlan,
    pub(super) result_limit: usize,
}

impl DescribeStreamsGroupSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        DescribeStreamsGroupPlan,
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

pub(crate) enum DescribeStreamsGroupTurn {
    Idle,
    Progress,
    Submit(DescribeStreamsGroupSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DescribeStreamsGroupHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
