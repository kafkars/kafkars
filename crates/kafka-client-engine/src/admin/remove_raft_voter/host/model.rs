//! Prepared voter identity and exact driver-handoff ownership.

use kafka_client_core::{OperationId, RemoveRaftVoterPlan};

use crate::clock::OperationDeadline;

/// Validated API81 request ready for the later engine-host adapter.
pub(crate) struct RemoveRaftVoterSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: RemoveRaftVoterPlan,
    pub(super) result_limit: usize,
}

impl RemoveRaftVoterSubmission {
    pub(crate) fn into_parts(self) -> (OperationId, OperationDeadline, RemoveRaftVoterPlan, usize) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.result_limit,
        )
    }
}

pub(crate) enum RemoveRaftVoterTurn {
    Idle,
    Progress,
    Submit(RemoveRaftVoterSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RemoveRaftVoterHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
