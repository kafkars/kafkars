//! Concrete operation, handoff, and submission ownership states.

use kafka_client_core::OperationId;

use crate::clock::OperationDeadline;

/// Exact discovery or broker call ready for driver admission.
pub(crate) enum ListConsumerGroupsSubmissionKind {
    Discovery,
    Broker { broker_id: i32 },
}

pub(crate) struct ListConsumerGroupsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) kind: ListConsumerGroupsSubmissionKind,
}

impl ListConsumerGroupsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        ListConsumerGroupsSubmissionKind,
    ) {
        (self.operation_id, self.deadline, self.kind)
    }
}

pub(crate) enum ListConsumerGroupsTurn {
    Idle,
    Progress,
    Submit(ListConsumerGroupsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ListConsumerGroupsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
