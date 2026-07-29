//! Bounded ownership of accepted Admin `DescribeMetadataQuorum` machines and calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    DescribeMetadataQuorumEffect, DescribeMetadataQuorumInput, DescribeMetadataQuorumMachine,
    DescribeMetadataQuorumTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminDescribeMetadataQuorumPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{DescribeMetadataQuorumCall, DescribeMetadataQuorumRawTerminal},
};

use super::{DescribeMetadataQuorumHostError, DescribeMetadataQuorumObserver};

pub(crate) const DESCRIBE_METADATA_QUORUM_CAPACITY: usize = 16;
pub(crate) const DESCRIBE_METADATA_QUORUM_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct DescribeMetadataQuorumAdmission {
    pub(crate) observer: DescribeMetadataQuorumObserver,
    pub(crate) fault: Option<DescribeMetadataQuorumHostError>,
}

/// One fixed query ready for the engine's driver-admission stage.
pub(crate) struct DescribeMetadataQuorumSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    result_limit: usize,
}

impl DescribeMetadataQuorumSubmission {
    pub(crate) const fn into_parts(self) -> (OperationId, OperationDeadline, usize) {
        (self.operation_id, self.deadline, self.result_limit)
    }
}

pub(crate) enum DescribeMetadataQuorumTurn {
    Idle,
    Progress,
    Submit(DescribeMetadataQuorumSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DescribeMetadataQuorumHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct DescribeMetadataQuorumOperation {
    operation_id: OperationId,
    machine: DescribeMetadataQuorumMachine,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    remaining_result_bytes: usize,
    submission: Option<DescribeMetadataQuorumSubmission>,
    handoff: DescribeMetadataQuorumHandoff,
    call: Option<DescribeMetadataQuorumCall>,
    recovered_call: Option<crate::driver::RecoveredDescribeMetadataQuorumCall>,
    raw_terminal: Option<DescribeMetadataQuorumRawTerminal>,
    terminal: Option<DescribeMetadataQuorumTerminal>,
}

pub(crate) struct DescribeMetadataQuorumHost {
    operations: Vec<DescribeMetadataQuorumOperation>,
    completions:
        CompletionRegistry<DescribeMetadataQuorumTerminal, AdminDescribeMetadataQuorumPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DescribeMetadataQuorumHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeMetadataQuorumHost {
    pub(crate) fn new(publisher: AdminDescribeMetadataQuorumPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_METADATA_QUORUM_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                DESCRIBE_METADATA_QUORUM_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_METADATA_QUORUM_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DescribeMetadataQuorumTurn, DescribeMetadataQuorumHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DescribeMetadataQuorumTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeMetadataQuorumTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, DescribeMetadataQuorumInput::DeadlineElapsed)?;
            return Ok(DescribeMetadataQuorumTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DescribeMetadataQuorumHostError::MissingSubmission)?;
        self.operations[index].handoff = DescribeMetadataQuorumHandoff::HandedOff;
        Ok(DescribeMetadataQuorumTurn::Submit(submission))
    }

    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DescribeMetadataQuorumCall,
    ) -> Result<(), DescribeMetadataQuorumHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeMetadataQuorumHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeMetadataQuorumHandoff::HandedOff
            || self.operations[index].call.is_some()
        {
            return Err(DescribeMetadataQuorumHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        self.apply(operation_id, DescribeMetadataQuorumInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), DescribeMetadataQuorumHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeMetadataQuorumHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeMetadataQuorumHandoff::HandedOff {
            return Err(DescribeMetadataQuorumHostError::InvalidHandoff);
        }
        self.apply(operation_id, DescribeMetadataQuorumInput::DriverRejected)
    }

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

    fn operation_index(&self, operation_id: OperationId) -> Option<usize> {
        self.operations
            .iter()
            .position(|operation| operation.operation_id == operation_id)
    }

    fn apply(
        &mut self,
        operation_id: OperationId,
        input: DescribeMetadataQuorumInput,
    ) -> Result<(), DescribeMetadataQuorumHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeMetadataQuorumHostError::UnknownOperation)?;
        let accepted = matches!(&input, DescribeMetadataQuorumInput::DriverAccepted);
        if accepted && self.operations[index].handoff != DescribeMetadataQuorumHandoff::HandedOff {
            return Err(DescribeMetadataQuorumHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DescribeMetadataQuorumHandoff::Submitted;
        }
        if let Some(DescribeMetadataQuorumEffect::Complete { terminal, .. }) =
            transition.into_effect()
        {
            self.operations[index].terminal = Some(terminal);
            self.publish_terminal(index)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}
