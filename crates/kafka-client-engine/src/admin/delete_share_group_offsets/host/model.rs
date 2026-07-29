//! Prepared API-92 request and exact driver-handoff ownership.

use kafka_client_core::{DeleteShareGroupOffsetsPlan, OperationId};

use crate::clock::OperationDeadline;

/// Validated API-92 v0 request ready for the engine-host adapter.
pub(crate) struct DeleteShareGroupOffsetsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: DeleteShareGroupOffsetsPlan,
    pub(super) result_limit: usize,
}

impl DeleteShareGroupOffsetsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        DeleteShareGroupOffsetsPlan,
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

pub(crate) enum DeleteShareGroupOffsetsTurn {
    Idle,
    Progress,
    Submit(DeleteShareGroupOffsetsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeleteShareGroupOffsetsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
