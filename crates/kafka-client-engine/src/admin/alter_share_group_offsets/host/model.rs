//! Prepared API-91 request and exact driver-handoff ownership.

use kafka_client_core::{AlterShareGroupOffsetsPlan, OperationId};

use crate::clock::OperationDeadline;

/// Validated API-91 v0 request ready for the engine-host adapter.
pub(crate) struct AlterShareGroupOffsetsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: AlterShareGroupOffsetsPlan,
    pub(super) result_limit: usize,
}

impl AlterShareGroupOffsetsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        AlterShareGroupOffsetsPlan,
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

pub(crate) enum AlterShareGroupOffsetsTurn {
    Idle,
    Progress,
    Submit(AlterShareGroupOffsetsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AlterShareGroupOffsetsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
