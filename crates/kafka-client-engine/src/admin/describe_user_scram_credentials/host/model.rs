//! Exact SCRAM description operation, attempt bounds, and driver evidence.

use kafka_client_core::{
    DescribeUserScramCredentialsMachine, DescribeUserScramCredentialsPlan,
    DescribeUserScramCredentialsState, DescribeUserScramCredentialsTerminal, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{
        DescribeUserScramCredentialsCall, DescribeUserScramCredentialsRawTerminal,
        RecoveredDescribeUserScramCredentialsCall,
    },
};

use super::super::{DescribeUserScramCredentialsHostError, DescribeUserScramCredentialsObserver};

pub(crate) struct DescribeUserScramCredentialsAdmission {
    pub(crate) observer: DescribeUserScramCredentialsObserver,
    pub(crate) fault: Option<DescribeUserScramCredentialsHostError>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DescribeUserScramCredentialsAttemptBounds {
    pub(super) request_limit: usize,
    pub(super) result_limit: usize,
}

/// One exact user selection ready for the engine's driver-admission stage.
pub(crate) struct DescribeUserScramCredentialsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) plan: DescribeUserScramCredentialsPlan,
    pub(super) bounds: DescribeUserScramCredentialsAttemptBounds,
}

impl DescribeUserScramCredentialsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        DescribeUserScramCredentialsPlan,
        usize,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.plan,
            self.bounds.request_limit,
            self.bounds.result_limit,
        )
    }
}

pub(crate) enum DescribeUserScramCredentialsTurn {
    Idle,
    Progress,
    Submit(DescribeUserScramCredentialsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DescribeUserScramCredentialsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct DescribeUserScramCredentialsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: DescribeUserScramCredentialsMachine,
    pub(super) expected_plan: DescribeUserScramCredentialsPlan,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) bounds: DescribeUserScramCredentialsAttemptBounds,
    pub(super) remaining_result_bytes: usize,
    pub(super) submission: Option<DescribeUserScramCredentialsSubmission>,
    pub(super) handoff: DescribeUserScramCredentialsHandoff,
    pub(super) call: Option<DescribeUserScramCredentialsCall>,
    pub(super) recovered_call: Option<RecoveredDescribeUserScramCredentialsCall>,
    pub(super) raw_terminal: Option<DescribeUserScramCredentialsRawTerminal>,
    pub(super) terminal: Option<DescribeUserScramCredentialsTerminal>,
}

impl DescribeUserScramCredentialsOperation {
    pub(super) fn matches_evidence(
        &self,
        plan: &DescribeUserScramCredentialsPlan,
        request_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.machine.state() == DescribeUserScramCredentialsState::AwaitingDriver
            && self.expected_plan == *plan
            && self.bounds
                == (DescribeUserScramCredentialsAttemptBounds {
                    request_limit,
                    result_limit,
                })
            && self.remaining_result_bytes == self.bounds.result_limit
    }

    pub(super) fn matches_call(&self, call: &DescribeUserScramCredentialsCall) -> bool {
        self.machine.state() == DescribeUserScramCredentialsState::AwaitingDriver
            && call.matches_evidence(
                &self.expected_plan,
                self.bounds.request_limit,
                self.bounds.result_limit,
            )
            && self.remaining_result_bytes == self.bounds.result_limit
    }

    pub(super) fn matches_recovered(
        &self,
        recovered: &RecoveredDescribeUserScramCredentialsCall,
    ) -> bool {
        matches!(
            self.machine.state(),
            DescribeUserScramCredentialsState::AwaitingDriver
                | DescribeUserScramCredentialsState::Submitted
        ) && recovered.matches_evidence(
            &self.expected_plan,
            self.bounds.request_limit,
            self.bounds.result_limit,
        ) && self.remaining_result_bytes == self.bounds.result_limit
    }

    pub(super) fn matches_raw(&self, raw: &DescribeUserScramCredentialsRawTerminal) -> bool {
        self.machine.state() == DescribeUserScramCredentialsState::Submitted
            && raw.matches_evidence(
                &self.expected_plan,
                self.bounds.request_limit,
                self.bounds.result_limit,
            )
            && self.remaining_result_bytes == self.bounds.result_limit
    }
}
