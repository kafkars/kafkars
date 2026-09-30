//! Prepared broker identity and exact driver-handoff ownership.

use kafka_client_core::{OperationId, UnregisterBrokerPlan};

use crate::clock::OperationDeadline;

/// Validated API64 request ready for the later engine-host adapter.
pub(crate) struct UnregisterBrokerSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: UnregisterBrokerPlan,
    pub(super) result_limit: usize,
    pub(super) controller_retry: bool,
}

impl UnregisterBrokerSubmission {
    pub(crate) const fn controller_retry(&self) -> bool {
        self.controller_retry
    }

    pub(crate) const fn into_parts(
        self,
    ) -> (OperationId, OperationDeadline, UnregisterBrokerPlan, usize) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.result_limit,
        )
    }
}

pub(crate) enum UnregisterBrokerTurn {
    Idle,
    Progress,
    Submit(UnregisterBrokerSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnregisterBrokerHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
