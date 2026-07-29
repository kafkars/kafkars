//! Prepared resource-type request and exact driver-handoff ownership.

use kafka_client_core::{ListConfigResourcesPlan, OperationId};

use crate::clock::OperationDeadline;

/// Validated API74 v1 request ready for the later engine-host adapter.
pub(crate) struct ListConfigResourcesSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: ListConfigResourcesPlan,
    pub(super) result_limit: usize,
}

impl ListConfigResourcesSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        ListConfigResourcesPlan,
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

pub(crate) enum ListConfigResourcesTurn {
    Idle,
    Progress,
    Submit(ListConfigResourcesSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ListConfigResourcesHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
