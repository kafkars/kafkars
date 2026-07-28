//! Exact grouped mutation, attempt bounds, and driver evidence ownership.

use kafka_client_core::{
    AlterReplicaLogDirAssignment, AlterReplicaLogDirsMachine, AlterReplicaLogDirsState,
    AlterReplicaLogDirsTerminal, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{
        AlterReplicaLogDirsCall, AlterReplicaLogDirsRawTerminal, RecoveredAlterReplicaLogDirsCall,
    },
};

use super::super::{AlterReplicaLogDirsHostError, AlterReplicaLogDirsObserver};

pub(crate) struct AlterReplicaLogDirsAdmission {
    pub(crate) observer: AlterReplicaLogDirsObserver,
    pub(crate) fault: Option<AlterReplicaLogDirsHostError>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AlterReplicaLogDirsAttemptBounds {
    pub(super) request_scratch_limit: usize,
    pub(super) result_limit: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct AlterReplicaLogDirsAttempt {
    pub(super) broker_id: i32,
    pub(super) assignments: Vec<AlterReplicaLogDirAssignment>,
    pub(super) bounds: AlterReplicaLogDirsAttemptBounds,
}

/// One exact broker group ready for request construction and driver admission.
pub(crate) struct AlterReplicaLogDirsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) broker_id: i32,
    pub(super) assignments: Vec<AlterReplicaLogDirAssignment>,
    pub(super) bounds: AlterReplicaLogDirsAttemptBounds,
}

impl AlterReplicaLogDirsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        i32,
        Vec<AlterReplicaLogDirAssignment>,
        usize,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.broker_id,
            self.assignments,
            self.bounds.request_scratch_limit,
            self.bounds.result_limit,
        )
    }
}

pub(crate) enum AlterReplicaLogDirsTurn {
    Idle,
    Progress,
    Submit(AlterReplicaLogDirsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AlterReplicaLogDirsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct AlterReplicaLogDirsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: AlterReplicaLogDirsMachine,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) remaining_result_bytes: usize,
    pub(super) submission: Option<AlterReplicaLogDirsSubmission>,
    pub(super) attempt: Option<AlterReplicaLogDirsAttempt>,
    pub(super) handoff: AlterReplicaLogDirsHandoff,
    pub(super) call: Option<AlterReplicaLogDirsCall>,
    pub(super) recovered_call: Option<RecoveredAlterReplicaLogDirsCall>,
    pub(super) raw_terminal: Option<AlterReplicaLogDirsRawTerminal>,
    pub(super) terminal: Option<AlterReplicaLogDirsTerminal>,
}

impl AlterReplicaLogDirsOperation {
    pub(super) fn matches_evidence(
        &self,
        broker_id: i32,
        assignments: &[AlterReplicaLogDirAssignment],
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.attempt.as_ref().is_some_and(|attempt| {
            attempt.broker_id == broker_id
                && attempt.assignments == assignments
                && attempt.bounds
                    == (AlterReplicaLogDirsAttemptBounds {
                        request_scratch_limit,
                        result_limit,
                    })
                && self.remaining_result_bytes == attempt.bounds.result_limit
        })
    }

    pub(super) fn matches_call(&self, call: &AlterReplicaLogDirsCall) -> bool {
        let Some(attempt) = self.attempt.as_ref() else {
            return false;
        };
        self.machine.state() == AlterReplicaLogDirsState::AwaitingDriver
            && call.matches_evidence(
                attempt.broker_id,
                &attempt.assignments,
                attempt.bounds.request_scratch_limit,
                attempt.bounds.result_limit,
            )
            && self.remaining_result_bytes == attempt.bounds.result_limit
    }

    pub(super) fn matches_recovered(&self, recovered: &RecoveredAlterReplicaLogDirsCall) -> bool {
        let Some(attempt) = self.attempt.as_ref() else {
            return false;
        };
        matches!(
            self.machine.state(),
            AlterReplicaLogDirsState::AwaitingDriver | AlterReplicaLogDirsState::Submitted
        ) && recovered.matches_evidence(
            attempt.broker_id,
            &attempt.assignments,
            attempt.bounds.request_scratch_limit,
            attempt.bounds.result_limit,
        ) && self.remaining_result_bytes == attempt.bounds.result_limit
    }

    pub(super) fn matches_raw(&self, raw: &AlterReplicaLogDirsRawTerminal) -> bool {
        let Some(attempt) = self.attempt.as_ref() else {
            return false;
        };
        self.machine.state() == AlterReplicaLogDirsState::Submitted
            && raw.matches_evidence(
                attempt.broker_id,
                &attempt.assignments,
                attempt.bounds.request_scratch_limit,
                attempt.bounds.result_limit,
            )
            && self.remaining_result_bytes == attempt.bounds.result_limit
    }
}
