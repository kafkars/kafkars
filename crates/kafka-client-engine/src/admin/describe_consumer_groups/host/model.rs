//! Caller route plan, exact attempt, and driver evidence ownership.

use kafka_client_core::{
    AdminDescribeConsumerGroupsCallKind, AdminDescribeConsumerGroupsMachine,
    AdminDescribeConsumerGroupsPlan, AdminDescribeConsumerGroupsState,
    AdminDescribeConsumerGroupsTerminal, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{
        DescribeConsumerGroupsCall, DescribeConsumerGroupsTerminal as RawTerminal,
        RecoveredDescribeConsumerGroupsCall,
    },
};

#[derive(Debug, Eq, PartialEq)]
pub(super) struct DescribeConsumerGroupsRoutePlan {
    groups: Vec<String>,
    include_authorized_operations: bool,
}

impl DescribeConsumerGroupsRoutePlan {
    pub(super) fn from_plan(plan: &AdminDescribeConsumerGroupsPlan) -> Self {
        Self {
            groups: plan.groups().to_vec(),
            include_authorized_operations: plan.include_authorized_operations(),
        }
    }

    pub(super) fn group(&self, index: usize) -> Option<&str> {
        self.groups.get(index).map(String::as_str)
    }

    #[cfg(test)]
    pub(super) fn groups(&self) -> &[String] {
        &self.groups
    }

    pub(super) const fn include_authorized_operations(&self) -> bool {
        self.include_authorized_operations
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DescribeConsumerGroupsAttemptBounds {
    pub(super) request_scratch_limit: usize,
    pub(super) result_limit: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct DescribeConsumerGroupsAttempt {
    pub(super) group_id: String,
    pub(super) include_authorized_operations: bool,
    pub(super) call_kind: AdminDescribeConsumerGroupsCallKind,
    pub(super) bounds: DescribeConsumerGroupsAttemptBounds,
}

/// One exact group ready for coordinator-call admission.
pub(crate) struct DescribeConsumerGroupsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) group_id: String,
    pub(super) include_authorized_operations: bool,
    pub(super) call_kind: AdminDescribeConsumerGroupsCallKind,
    pub(super) bounds: DescribeConsumerGroupsAttemptBounds,
}

impl DescribeConsumerGroupsSubmission {
    #[allow(clippy::type_complexity)]
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        String,
        bool,
        AdminDescribeConsumerGroupsCallKind,
        usize,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.group_id,
            self.include_authorized_operations,
            self.call_kind,
            self.bounds.request_scratch_limit,
            self.bounds.result_limit,
        )
    }
}

pub(crate) enum DescribeConsumerGroupsTurn {
    Idle,
    Progress,
    Submit(DescribeConsumerGroupsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DescribeConsumerGroupsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct DescribeConsumerGroupsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: AdminDescribeConsumerGroupsMachine,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) remaining_result_bytes: usize,
    pub(super) route_plan: DescribeConsumerGroupsRoutePlan,
    pub(super) route_index: usize,
    pub(super) submission: Option<DescribeConsumerGroupsSubmission>,
    pub(super) attempt: Option<DescribeConsumerGroupsAttempt>,
    pub(super) handoff: DescribeConsumerGroupsHandoff,
    pub(super) call: Option<DescribeConsumerGroupsCall>,
    pub(super) recovered_call: Option<RecoveredDescribeConsumerGroupsCall>,
    pub(super) raw_terminal: Option<RawTerminal>,
    pub(super) terminal: Option<AdminDescribeConsumerGroupsTerminal>,
}

impl DescribeConsumerGroupsOperation {
    pub(super) fn matches_evidence(
        &self,
        group_id: &str,
        include_authorized_operations: bool,
        call_kind: AdminDescribeConsumerGroupsCallKind,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.attempt.as_ref().is_some_and(|attempt| {
            attempt.group_id == group_id
                && attempt.include_authorized_operations == include_authorized_operations
                && attempt.call_kind == call_kind
                && attempt.bounds
                    == (DescribeConsumerGroupsAttemptBounds {
                        request_scratch_limit,
                        result_limit,
                    })
                && self.route_plan.group(self.route_index) == Some(group_id)
                && self.machine.current_group() == Some(group_id)
                && self.machine.include_authorized_operations() == include_authorized_operations
                && self.machine.call_kind() == call_kind
                && self.remaining_result_bytes == result_limit
        })
    }

    pub(super) fn matches_call(&self, call: &DescribeConsumerGroupsCall) -> bool {
        let Some(attempt) = self.attempt.as_ref() else {
            return false;
        };
        self.machine.state() == AdminDescribeConsumerGroupsState::AwaitingDriver
            && self.matches_evidence(
                &attempt.group_id,
                attempt.include_authorized_operations,
                attempt.call_kind,
                attempt.bounds.request_scratch_limit,
                attempt.bounds.result_limit,
            )
            && call.matches_evidence(
                &attempt.group_id,
                attempt.include_authorized_operations,
                attempt.call_kind,
                attempt.bounds.request_scratch_limit,
                attempt.bounds.result_limit,
            )
            && self.remaining_result_bytes == attempt.bounds.result_limit
    }

    pub(super) fn matches_recovered(
        &self,
        recovered: &RecoveredDescribeConsumerGroupsCall,
    ) -> bool {
        let Some(attempt) = self.attempt.as_ref() else {
            return false;
        };
        matches!(
            self.machine.state(),
            AdminDescribeConsumerGroupsState::AwaitingDriver
                | AdminDescribeConsumerGroupsState::Submitted
        ) && self.matches_evidence(
            &attempt.group_id,
            attempt.include_authorized_operations,
            attempt.call_kind,
            attempt.bounds.request_scratch_limit,
            attempt.bounds.result_limit,
        ) && recovered.matches_evidence(
            &attempt.group_id,
            attempt.include_authorized_operations,
            attempt.call_kind,
            attempt.bounds.request_scratch_limit,
            attempt.bounds.result_limit,
        )
    }

    pub(super) fn matches_raw(&self, raw: &RawTerminal) -> bool {
        let Some(attempt) = self.attempt.as_ref() else {
            return false;
        };
        self.machine.state() == AdminDescribeConsumerGroupsState::Submitted
            && self.matches_evidence(
                &attempt.group_id,
                attempt.include_authorized_operations,
                attempt.call_kind,
                attempt.bounds.request_scratch_limit,
                attempt.bounds.result_limit,
            )
            && raw.matches_evidence(
                &attempt.group_id,
                attempt.include_authorized_operations,
                attempt.call_kind,
                attempt.bounds.request_scratch_limit,
                attempt.bounds.result_limit,
            )
            && self.remaining_result_bytes == attempt.bounds.result_limit
    }
}
