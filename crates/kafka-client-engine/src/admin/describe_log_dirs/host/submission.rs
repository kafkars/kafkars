//! Exact-broker submission ownership and handoff state for `DescribeLogDirs`.

use kafka_client_core::{
    AdminDescribeLogDirsEffect, AdminDescribeLogDirsInput, AdminDescribeLogDirsSelection, Moment,
    OperationId,
};

use crate::clock::OperationDeadline;

use super::{DescribeLogDirsHost, DescribeLogDirsHostError};

/// One exact broker ready for the engine's driver-admission stage.
pub(crate) struct DescribeLogDirsSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    broker_id: i32,
    selection: AdminDescribeLogDirsSelection,
    request_scratch_limit: usize,
    result_limit: usize,
}

impl DescribeLogDirsSubmission {
    pub(super) const fn new(
        operation_id: OperationId,
        deadline: OperationDeadline,
        broker_id: i32,
        selection: AdminDescribeLogDirsSelection,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            operation_id,
            deadline,
            broker_id,
            selection,
            request_scratch_limit,
            result_limit,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        i32,
        AdminDescribeLogDirsSelection,
        usize,
        usize,
    ) {
        (
            self.operation_id,
            self.deadline,
            self.broker_id,
            self.selection,
            self.request_scratch_limit,
            self.result_limit,
        )
    }
}

pub(crate) enum DescribeLogDirsTurn {
    Idle,
    Progress,
    Submit(DescribeLogDirsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DescribeLogDirsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

impl DescribeLogDirsHost {
    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DescribeLogDirsTurn, DescribeLogDirsHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DescribeLogDirsTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeLogDirsTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, AdminDescribeLogDirsInput::DeadlineElapsed)?;
            return Ok(DescribeLogDirsTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DescribeLogDirsHostError::MissingSubmission)?;
        self.operations[index].handoff = DescribeLogDirsHandoff::HandedOff;
        Ok(DescribeLogDirsTurn::Submit(submission))
    }

    pub(super) fn validate_effect(
        &self,
        index: usize,
        effect: &AdminDescribeLogDirsEffect,
    ) -> Result<(), DescribeLogDirsHostError> {
        let operation = &self.operations[index];
        match effect {
            AdminDescribeLogDirsEffect::Submit {
                operation_id,
                deadline,
                broker_id,
                selection,
            } if *operation_id == operation.operation_id
                && *deadline == operation.deadline.core()
                && operation.machine.current_broker() == Some(*broker_id)
                && selection == operation.plan.selection() =>
            {
                Ok(())
            }
            AdminDescribeLogDirsEffect::Complete { operation_id, .. }
                if *operation_id == operation.operation_id =>
            {
                Ok(())
            }
            _ => Err(DescribeLogDirsHostError::SubmissionMismatch),
        }
    }

    pub(super) fn install_effect(
        &mut self,
        index: usize,
        effect: AdminDescribeLogDirsEffect,
    ) -> Result<(), DescribeLogDirsHostError> {
        self.validate_effect(index, &effect)?;
        let operation_id = self.operations[index].operation_id;
        match effect {
            AdminDescribeLogDirsEffect::Submit {
                operation_id: _,
                deadline: _,
                broker_id,
                selection,
            } => {
                self.operations[index].result_limit = self.operations[index].remaining_result_bytes;
                self.operations[index].submission = Some(DescribeLogDirsSubmission::new(
                    operation_id,
                    self.operations[index].deadline,
                    broker_id,
                    selection,
                    self.operations[index].request_scratch_limit,
                    self.operations[index].result_limit,
                ));
                self.operations[index].handoff = DescribeLogDirsHandoff::Untouched;
                Ok(())
            }
            AdminDescribeLogDirsEffect::Complete {
                operation_id: _,
                terminal,
            } => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
        }
    }
}
