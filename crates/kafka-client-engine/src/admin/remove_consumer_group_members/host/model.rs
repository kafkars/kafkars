//! Exact member-removal submission, handoff, and driver-evidence ownership.

use kafka_client_core::{
    OperationId, RemoveConsumerGroupMembersMachine, RemoveConsumerGroupMembersPlan,
    RemoveConsumerGroupMembersState, RemoveConsumerGroupMembersTerminal,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{
        RecoveredRemoveConsumerGroupMembersCall, RemoveConsumerGroupMembersCall,
        RemoveConsumerGroupMembersTerminal as RawTerminal,
    },
};

pub(crate) struct RemoveConsumerGroupMembersSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: RemoveConsumerGroupMembersPlan,
    pub(super) request_scratch_limit: usize,
    pub(super) result_limit: usize,
}

impl RemoveConsumerGroupMembersSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        RemoveConsumerGroupMembersPlan,
        usize,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.request_scratch_limit,
            self.result_limit,
        )
    }
}

pub(crate) enum RemoveConsumerGroupMembersTurn {
    Idle,
    Progress,
    Submit(RemoveConsumerGroupMembersSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RemoveConsumerGroupMembersHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct RemoveConsumerGroupMembersOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: RemoveConsumerGroupMembersMachine,
    pub(super) response_plan: RemoveConsumerGroupMembersPlan,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) request_scratch_limit: usize,
    pub(super) result_limit: usize,
    pub(super) submission: Option<RemoveConsumerGroupMembersSubmission>,
    pub(super) rejected_submission: Option<(RemoveConsumerGroupMembersPlan, usize, usize)>,
    pub(super) handoff: RemoveConsumerGroupMembersHandoff,
    pub(super) call: Option<RemoveConsumerGroupMembersCall>,
    pub(super) recovered_call: Option<RecoveredRemoveConsumerGroupMembersCall>,
    pub(super) raw_terminal: Option<RawTerminal>,
    pub(super) terminal: Option<RemoveConsumerGroupMembersTerminal>,
}

impl RemoveConsumerGroupMembersOperation {
    pub(super) fn matches_submission(
        &self,
        plan: &RemoveConsumerGroupMembersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.machine.state() == RemoveConsumerGroupMembersState::AwaitingDriver
            && self.response_plan == *plan
            && self.request_scratch_limit == request_scratch_limit
            && self.result_limit == result_limit
    }

    pub(super) fn matches_call(&self, call: &RemoveConsumerGroupMembersCall) -> bool {
        self.active()
            && call.matches(
                &self.response_plan,
                self.request_scratch_limit,
                self.result_limit,
            )
    }

    pub(super) fn matches_recovered(
        &self,
        recovered: &RecoveredRemoveConsumerGroupMembersCall,
    ) -> bool {
        self.active()
            && recovered.matches(
                &self.response_plan,
                self.request_scratch_limit,
                self.result_limit,
            )
    }

    pub(super) fn matches_raw(&self, raw: &RawTerminal) -> bool {
        self.machine.state() == RemoveConsumerGroupMembersState::Submitted
            && raw.matches(
                &self.response_plan,
                self.request_scratch_limit,
                self.result_limit,
            )
    }

    fn active(&self) -> bool {
        matches!(
            self.machine.state(),
            RemoveConsumerGroupMembersState::AwaitingDriver
                | RemoveConsumerGroupMembersState::Submitted
        )
    }
}
