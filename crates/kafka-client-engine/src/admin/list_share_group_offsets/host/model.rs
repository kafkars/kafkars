//! Prepared API-90 request and exact driver-handoff ownership.

use kafka_client_core::{ListShareGroupOffsetsPlan, OperationId};

use crate::clock::OperationDeadline;

/// Validated API-90 v0-v1 request ready for the engine-host adapter.
pub(crate) struct ListShareGroupOffsetsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: ListShareGroupOffsetsPlan,
    pub(super) result_limit: usize,
}

impl ListShareGroupOffsetsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        ListShareGroupOffsetsPlan,
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

pub(crate) enum ListShareGroupOffsetsTurn {
    Idle,
    Progress,
    Submit(ListShareGroupOffsetsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ListShareGroupOffsetsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
