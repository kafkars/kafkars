//! Exact assignment fence and unchanged deadline for one group position RPC.

use kafka_client_core::GroupPositionFence;

use crate::clock::OperationDeadline;

/// Linear correlation key for one accepted assignment-fenced `OffsetFetch`.
#[must_use = "a group position call key must be submitted or terminally settled"]
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct GroupPositionOffsetFetchKey {
    fence: GroupPositionFence,
    operation_deadline: OperationDeadline,
}

impl GroupPositionOffsetFetchKey {
    pub(crate) const fn new(
        fence: GroupPositionFence,
        operation_deadline: OperationDeadline,
    ) -> Self {
        Self {
            fence,
            operation_deadline,
        }
    }

    pub(crate) const fn fence(&self) -> GroupPositionFence {
        self.fence
    }

    pub(crate) const fn operation_deadline(&self) -> OperationDeadline {
        self.operation_deadline
    }
}
