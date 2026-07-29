//! Exact-broker submission ownership and handoff state for `DescribeLogDirs`.

use kafka_client_core::{AdminDescribeLogDirsSelection, OperationId};

use crate::clock::OperationDeadline;

/// One exact broker ready for the engine's driver-admission stage.
pub(crate) struct DescribeLogDirsSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    broker_id: i32,
    selection: AdminDescribeLogDirsSelection,
    request_retained_limit: usize,
}

impl DescribeLogDirsSubmission {
    pub(super) const fn new(
        operation_id: OperationId,
        deadline: OperationDeadline,
        broker_id: i32,
        selection: AdminDescribeLogDirsSelection,
        request_retained_limit: usize,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            broker_id,
            selection,
            request_retained_limit,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        i32,
        AdminDescribeLogDirsSelection,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.broker_id,
            self.selection,
            self.request_retained_limit,
        )
    }
}

pub(crate) enum DescribeLogDirsTurn {
    Idle,
    Progress,
    Submit(DescribeLogDirsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DescribeLogDirsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
