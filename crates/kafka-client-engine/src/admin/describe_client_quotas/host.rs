//! Bounded ownership of accepted Admin `DescribeClientQuotas` machines and calls.

mod admission;
mod response;
mod terminal;

#[cfg(test)]
mod response_test;

use kafka_client_core::{
    DescribeClientQuotasEffect, DescribeClientQuotasInput, DescribeClientQuotasMachine,
    DescribeClientQuotasPlan, DescribeClientQuotasTerminal, Moment, OperationId,
};

use crate::{
    admin::AdminDescribeClientQuotasPublisher,
    clock::OperationDeadline,
    completion::{CompletionId, CompletionRegistry},
    driver::{
        DescribeClientQuotasCall, DescribeClientQuotasRawTerminal,
        RecoveredDescribeClientQuotasCall,
    },
};

use super::{DescribeClientQuotasHostError, DescribeClientQuotasObserver};

pub(crate) const DESCRIBE_CLIENT_QUOTAS_CAPACITY: usize = 16;
pub(crate) const DESCRIBE_CLIENT_QUOTAS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct DescribeClientQuotasAdmission {
    pub(crate) observer: DescribeClientQuotasObserver,
    pub(crate) fault: Option<DescribeClientQuotasHostError>,
}

/// One exact filter ready for the engine's driver-admission stage.
pub(crate) struct DescribeClientQuotasSubmission {
    operation_id: OperationId,
    deadline: OperationDeadline,
    plan: DescribeClientQuotasPlan,
    request_scratch_limit: usize,
    result_limit: usize,
}

impl DescribeClientQuotasSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        DescribeClientQuotasPlan,
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

pub(crate) enum DescribeClientQuotasTurn {
    Idle,
    Progress,
    Submit(DescribeClientQuotasSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DescribeClientQuotasHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

struct DescribeClientQuotasOperation {
    operation_id: OperationId,
    machine: DescribeClientQuotasMachine,
    plan: DescribeClientQuotasPlan,
    completion_id: CompletionId,
    deadline: OperationDeadline,
    retained_bytes: usize,
    request_scratch_limit: usize,
    result_limit: usize,
    remaining_result_bytes: usize,
    submission: Option<DescribeClientQuotasSubmission>,
    rejected_submission: Option<(DescribeClientQuotasPlan, usize, usize)>,
    handoff: DescribeClientQuotasHandoff,
    call: Option<DescribeClientQuotasCall>,
    // Driver-shutdown proof remains live until core accepts the terminal fact.
    recovered_call: Option<RecoveredDescribeClientQuotasCall>,
    raw_terminal: Option<DescribeClientQuotasRawTerminal>,
    terminal: Option<DescribeClientQuotasTerminal>,
}

pub(crate) struct DescribeClientQuotasHost {
    operations: Vec<DescribeClientQuotasOperation>,
    completions:
        CompletionRegistry<DescribeClientQuotasTerminal, AdminDescribeClientQuotasPublisher>,
    next_operation_id: Option<OperationId>,
    reclaim_pending: Option<CompletionId>,
    retained_bytes: usize,
    accepting: bool,
    health: Option<DescribeClientQuotasHostError>,
    published_bytes: Vec<(CompletionId, usize)>,
}

impl DescribeClientQuotasHost {
    pub(crate) fn new(publisher: AdminDescribeClientQuotasPublisher) -> Self {
        Self {
            operations: Vec::with_capacity(DESCRIBE_CLIENT_QUOTAS_CAPACITY),
            completions: CompletionRegistry::with_publisher(
                DESCRIBE_CLIENT_QUOTAS_CAPACITY,
                publisher,
            ),
            next_operation_id: Some(OperationId::from_raw(1)),
            reclaim_pending: None,
            retained_bytes: 0,
            accepting: true,
            health: None,
            published_bytes: Vec::with_capacity(DESCRIBE_CLIENT_QUOTAS_CAPACITY),
        }
    }

    pub(crate) fn turn(
        &mut self,
        now: Moment,
    ) -> Result<DescribeClientQuotasTurn, DescribeClientQuotasHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.reclaim_one()? || self.poll_one_call()? {
            return Ok(DescribeClientQuotasTurn::Progress);
        }
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.submission.is_some())
        else {
            return Ok(DescribeClientQuotasTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            let operation_id = self.operations[index].operation_id;
            self.apply(operation_id, DescribeClientQuotasInput::DeadlineElapsed)?;
            return Ok(DescribeClientQuotasTurn::Progress);
        }
        let submission = self.operations[index]
            .submission
            .take()
            .ok_or(DescribeClientQuotasHostError::MissingSubmission)?;
        self.operations[index].handoff = DescribeClientQuotasHandoff::HandedOff;
        Ok(DescribeClientQuotasTurn::Submit(submission))
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
        input: DescribeClientQuotasInput,
    ) -> Result<(), DescribeClientQuotasHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeClientQuotasHostError::UnknownOperation)?;
        let accepted = matches!(&input, DescribeClientQuotasInput::DriverAccepted);
        if accepted && self.operations[index].handoff != DescribeClientQuotasHandoff::HandedOff {
            return Err(DescribeClientQuotasHostError::InvalidHandoff);
        }
        let transition = self.operations[index].machine.apply(input)?;
        if accepted {
            self.operations[index].handoff = DescribeClientQuotasHandoff::Submitted;
        }
        match transition.into_effect() {
            Some(DescribeClientQuotasEffect::Complete {
                operation_id: effect_id,
                terminal,
            }) if effect_id == operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)?;
            }
            Some(_) => return Err(DescribeClientQuotasHostError::SubmissionMismatch),
            None => {}
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) const fn retained_bytes_for_test(&self) -> usize {
        self.retained_bytes
    }
}
