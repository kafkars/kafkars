//! Submission, handoff, and operation ownership vocabulary.

use kafka_client_core::{
    DeleteConsumerGroupsMachine, DeleteConsumerGroupsPlan, DeleteConsumerGroupsTarget,
    DeleteConsumerGroupsTerminal, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{
        DeleteConsumerGroupsCall, DeleteConsumerGroupsRawTerminal,
        RecoveredDeleteConsumerGroupsCall,
    },
};

use super::DeleteConsumerGroupsHost;

/// One exact target ready for the engine's driver-admission stage.
pub(crate) struct DeleteConsumerGroupsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: DeleteConsumerGroupsPlan,
    pub(super) target: DeleteConsumerGroupsTarget,
    pub(super) request_limit: usize,
    pub(super) result_limit: usize,
}

impl DeleteConsumerGroupsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        DeleteConsumerGroupsPlan,
        DeleteConsumerGroupsTarget,
        usize,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.target,
            self.request_limit,
            self.result_limit,
        )
    }
}

pub(crate) enum DeleteConsumerGroupsTurn {
    Idle,
    Progress,
    Submit(DeleteConsumerGroupsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeleteConsumerGroupsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct DeleteConsumerGroupsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: DeleteConsumerGroupsMachine,
    pub(super) plan: DeleteConsumerGroupsPlan,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) request_limit: usize,
    pub(super) result_limit: usize,
    pub(super) remaining_result_bytes: usize,
    pub(super) submission: Option<DeleteConsumerGroupsSubmission>,
    pub(super) rejected_submission: Option<(
        DeleteConsumerGroupsPlan,
        DeleteConsumerGroupsTarget,
        usize,
        usize,
    )>,
    pub(super) handoff: DeleteConsumerGroupsHandoff,
    pub(super) call: Option<DeleteConsumerGroupsCall>,
    pub(super) recovered_call: Option<RecoveredDeleteConsumerGroupsCall>,
    pub(super) raw_terminal: Option<DeleteConsumerGroupsRawTerminal>,
    pub(super) terminal: Option<DeleteConsumerGroupsTerminal>,
}

impl DeleteConsumerGroupsOperation {
    pub(super) fn matches_evidence(
        &self,
        plan: &DeleteConsumerGroupsPlan,
        target: &DeleteConsumerGroupsTarget,
        request_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.plan == *plan
            && self.machine.current_target() == Some(target)
            && self.request_limit == request_limit
            && self.result_limit == result_limit
    }

    pub(super) fn call_matches_expected(&self) -> bool {
        let (Some(target), Some(call)) = (self.machine.current_target(), self.call.as_ref()) else {
            return false;
        };
        call.matches_evidence(&self.plan, target, self.request_limit, self.result_limit)
    }
}

impl DeleteConsumerGroupsHost {
    pub(crate) fn close_admission(&mut self) {
        self.accepting = false;
    }

    pub(crate) fn unsettled(&self) -> usize {
        self.operations.len()
    }

    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.operations
            .iter()
            .filter(|operation| operation.submission.is_some())
            .map(|operation| operation.deadline.core())
            .min()
    }

    pub(super) fn operation_index(&self, operation_id: OperationId) -> Option<usize> {
        self.operations
            .iter()
            .position(|operation| operation.operation_id == operation_id)
    }
}
