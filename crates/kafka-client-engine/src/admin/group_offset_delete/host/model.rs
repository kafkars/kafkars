//! Exact operation, handoff, and submission owners for offset deletion.

use kafka_client_core::{
    DeleteConsumerGroupOffsetsMachine, DeleteConsumerGroupOffsetsPlan,
    DeleteConsumerGroupOffsetsTerminal, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{GroupOffsetDeleteCall, GroupOffsetDeleteTerminal, RecoveredGroupOffsetDeleteCall},
};

pub(crate) struct DeleteConsumerGroupOffsetsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: DeleteConsumerGroupOffsetsPlan,
    pub(super) request_scratch_limit: usize,
}

impl DeleteConsumerGroupOffsetsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        DeleteConsumerGroupOffsetsPlan,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.request_scratch_limit,
        )
    }
}

pub(crate) enum DeleteConsumerGroupOffsetsTurn {
    Idle,
    Progress,
    Submit(DeleteConsumerGroupOffsetsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeleteConsumerGroupOffsetsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct DeleteConsumerGroupOffsetsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: DeleteConsumerGroupOffsetsMachine,
    pub(super) response_plan: DeleteConsumerGroupOffsetsPlan,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) result_limit: usize,
    pub(super) submission: Option<DeleteConsumerGroupOffsetsSubmission>,
    pub(super) handoff: DeleteConsumerGroupOffsetsHandoff,
    pub(super) call: Option<GroupOffsetDeleteCall>,
    pub(super) recovered_call: Option<RecoveredGroupOffsetDeleteCall>,
    pub(super) raw_terminal: Option<GroupOffsetDeleteTerminal>,
    pub(super) terminal: Option<DeleteConsumerGroupOffsetsTerminal>,
}

impl DeleteConsumerGroupOffsetsOperation {
    pub(super) fn mark_handed_off(&mut self) {
        self.handoff = DeleteConsumerGroupOffsetsHandoff::HandedOff;
    }

    pub(super) fn mark_submitted(&mut self) {
        self.handoff = DeleteConsumerGroupOffsetsHandoff::Submitted;
    }
}
