//! Prepared neutral submission and driver handoff ownership.

use kafka_client_core::{DescribeTopicPartitionsPlan, OperationId};

use crate::clock::OperationDeadline;

/// Generated-free one-page plan ready for the later engine-host adapter.
pub(crate) struct AdminDescribeTopicPartitionsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: DescribeTopicPartitionsPlan,
    pub(super) retained_limit: usize,
}

impl AdminDescribeTopicPartitionsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        DescribeTopicPartitionsPlan,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.retained_limit,
        )
    }
}

pub(crate) enum AdminDescribeTopicPartitionsTurn {
    Idle,
    Progress,
    Submit(AdminDescribeTopicPartitionsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AdminDescribeTopicPartitionsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}
