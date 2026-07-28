//! Exact submission, handoff, driver-evidence, and operation ownership.

use kafka_client_core::{
    ListPartitionReassignmentsMachine, ListPartitionReassignmentsPlan,
    ListPartitionReassignmentsState, ListPartitionReassignmentsTerminal, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{
        ListPartitionReassignmentsCall, ListPartitionReassignmentsRawTerminal,
        RecoveredListPartitionReassignmentsCall,
    },
};

pub(crate) struct ListPartitionReassignmentsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: ListPartitionReassignmentsPlan,
    pub(super) result_limit: usize,
}

impl ListPartitionReassignmentsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        ListPartitionReassignmentsPlan,
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

pub(crate) enum ListPartitionReassignmentsTurn {
    Idle,
    Progress,
    Submit(ListPartitionReassignmentsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ListPartitionReassignmentsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct ListPartitionReassignmentsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: ListPartitionReassignmentsMachine,
    pub(super) plan: ListPartitionReassignmentsPlan,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) result_limit: usize,
    pub(super) submission: Option<ListPartitionReassignmentsSubmission>,
    pub(super) rejected_submission: Option<(ListPartitionReassignmentsPlan, usize)>,
    pub(super) handoff: ListPartitionReassignmentsHandoff,
    pub(super) call: Option<ListPartitionReassignmentsCall>,
    pub(super) recovered_call: Option<RecoveredListPartitionReassignmentsCall>,
    pub(super) raw_terminal: Option<ListPartitionReassignmentsRawTerminal>,
    pub(super) terminal: Option<ListPartitionReassignmentsTerminal>,
}

impl ListPartitionReassignmentsOperation {
    pub(super) fn matches_submission(
        &self,
        plan: &ListPartitionReassignmentsPlan,
        result_limit: usize,
    ) -> bool {
        self.machine.state() == ListPartitionReassignmentsState::AwaitingDriver
            && self.plan == *plan
            && self.result_limit == result_limit
    }

    pub(super) fn matches_call(&self, call: &ListPartitionReassignmentsCall) -> bool {
        matches!(
            self.machine.state(),
            ListPartitionReassignmentsState::AwaitingDriver
                | ListPartitionReassignmentsState::Submitted
        ) && call.matches(&self.plan, self.result_limit)
    }

    pub(super) fn matches_recovered(
        &self,
        recovered: &RecoveredListPartitionReassignmentsCall,
    ) -> bool {
        matches!(
            self.machine.state(),
            ListPartitionReassignmentsState::AwaitingDriver
                | ListPartitionReassignmentsState::Submitted
        ) && recovered.matches(&self.plan, self.result_limit)
    }
}
