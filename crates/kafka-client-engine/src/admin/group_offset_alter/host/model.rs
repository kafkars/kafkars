//! Exact plan, capacity, submission, and terminal ownership for one alteration.

use kafka_client_core::{
    AlterConsumerGroupOffsetsMachine, AlterConsumerGroupOffsetsPlan,
    AlterConsumerGroupOffsetsState, AlterConsumerGroupOffsetsTerminal, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{GroupOffsetAlterCall, GroupOffsetAlterTerminal, RecoveredGroupOffsetAlterCall},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AlterConsumerGroupOffsetsBounds {
    pub(super) request_scratch_limit: usize,
    pub(super) result_limit: usize,
}

pub(crate) struct AlterConsumerGroupOffsetsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: AlterConsumerGroupOffsetsPlan,
    pub(super) bounds: AlterConsumerGroupOffsetsBounds,
}

impl AlterConsumerGroupOffsetsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        AlterConsumerGroupOffsetsPlan,
        usize,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.bounds.request_scratch_limit,
            self.bounds.result_limit,
        )
    }
}

pub(crate) enum AlterConsumerGroupOffsetsTurn {
    Idle,
    Progress,
    Submit(AlterConsumerGroupOffsetsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AlterConsumerGroupOffsetsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct AlterConsumerGroupOffsetsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: AlterConsumerGroupOffsetsMachine,
    pub(super) plan: AlterConsumerGroupOffsetsPlan,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) bounds: AlterConsumerGroupOffsetsBounds,
    pub(super) submission: Option<AlterConsumerGroupOffsetsSubmission>,
    pub(super) rejected_submission: Option<(
        AlterConsumerGroupOffsetsPlan,
        AlterConsumerGroupOffsetsBounds,
    )>,
    pub(super) handoff: AlterConsumerGroupOffsetsHandoff,
    pub(super) call: Option<GroupOffsetAlterCall>,
    pub(super) recovered_call: Option<RecoveredGroupOffsetAlterCall>,
    pub(super) raw_terminal: Option<GroupOffsetAlterTerminal>,
    pub(super) terminal: Option<AlterConsumerGroupOffsetsTerminal>,
}

impl AlterConsumerGroupOffsetsOperation {
    pub(super) fn matches_submission(
        &self,
        plan: &AlterConsumerGroupOffsetsPlan,
        bounds: AlterConsumerGroupOffsetsBounds,
    ) -> bool {
        self.machine.state() == AlterConsumerGroupOffsetsState::AwaitingDriver
            && self.plan == *plan
            && self.bounds == bounds
    }

    pub(super) fn matches_call(&self, call: &GroupOffsetAlterCall) -> bool {
        matches!(
            self.machine.state(),
            AlterConsumerGroupOffsetsState::AwaitingDriver
                | AlterConsumerGroupOffsetsState::Submitted
        ) && call.matches_evidence(
            &self.plan,
            self.bounds.request_scratch_limit,
            self.bounds.result_limit,
        )
    }

    pub(super) fn matches_recovered(&self, recovered: &RecoveredGroupOffsetAlterCall) -> bool {
        matches!(
            self.machine.state(),
            AlterConsumerGroupOffsetsState::AwaitingDriver
                | AlterConsumerGroupOffsetsState::Submitted
        ) && recovered.matches_evidence(
            &self.plan,
            self.bounds.request_scratch_limit,
            self.bounds.result_limit,
        )
    }

    pub(super) fn matches_raw(&self, raw: &GroupOffsetAlterTerminal) -> bool {
        self.machine.state() == AlterConsumerGroupOffsetsState::Submitted
            && raw.matches_evidence(
                &self.plan,
                self.bounds.request_scratch_limit,
                self.bounds.result_limit,
            )
    }

    pub(super) fn mark_handed_off(&mut self) {
        self.handoff = AlterConsumerGroupOffsetsHandoff::HandedOff;
    }

    pub(super) fn mark_submitted(&mut self) {
        self.handoff = AlterConsumerGroupOffsetsHandoff::Submitted;
    }
}
